import { pgListener, db } from '.'
import { getSpecTriggers, formatTriggerName, dropTrigger, createTrigger } from './triggers'
import { TableSub, TableSubStatus, TableSubEvent, StringKeyMap, StringMap, Trigger, TriggerEvent, TableLinkDataChanges, LiveObject } from '../types'
import { tablesMeta, getRel } from './tablesMeta'
import config from '../config'
import logger from '../logger'
import SeedTableService from '../services/SeedTableService'
import ResolveRecordsService from '../services/ResolveRecordsService'
import constants from '../constants'
import debounce from 'lodash.debounce'
import { QueryError } from '../errors'

export class TableSubscriber {

    tableSubs: { [key: string]: TableSub } = {}

    getLiveObject: (id: string) => LiveObject

    async upsertTableSubs() {
        // Create a map of table subs from the tables mentioned in the config.
        this._populateTableSubsFromConfig()

        // Find which table subs are pending (which ones we need to check on to make sure they exist).
        const pendingSubs = Object.values(this.tableSubs).filter(ts => (
            ts.status === TableSubStatus.Pending
        ))
        if (!pendingSubs.length) return

        // Get all existing spec triggers.
        let triggers
        try {
            triggers = await getSpecTriggers()
        } catch (err) {
            logger.error(`Failed to fetch spec triggers: ${err}`)
            return
        }

        // Map existing triggers by <schema>:<table>:<event>
        const existingTriggersMap = {}
        for (const trigger of triggers) {
            const { schema, table, event } = trigger
            const key = [schema, table, event].join(':')
            existingTriggersMap[key] = trigger
        }
        
        // Upsert all pending subs (their Postgres triggers and functions).
        await Promise.all(pendingSubs.map(ts => this._upsertTableSub(ts, existingTriggersMap)))

        // All subs that were successfully upserted in the previous step should now be in the subscribing state.
        const subsThatNeedSubscribing = Object.values(this.tableSubs).filter(ts => (
            ts.status === TableSubStatus.Subscribing
        ))
        if (!subsThatNeedSubscribing.length) {
            logger.warn('Not all pending table subs moved to the subscribing status...')
            return
        }

        // Subscribe to tables (listen for trigger notifications).
        await this._subscribeToTables(subsThatNeedSubscribing)
    }

    async _subscribeToTables(subsThatNeedSubscribing: TableSub[]) {
        // Register tables as subscribed.
        for (const { schema, table } of subsThatNeedSubscribing) {
            const tablePath = [schema, table].join('.')
            this.tableSubs[tablePath].status = TableSubStatus.Subscribed
            logger.info(`Listening for changes on table ${tablePath}...`)
        }

        // Ensure we're not already subscribed to the table subs channel.
        const subscribedChannels = pgListener.getSubscribedChannels()
        if (subscribedChannels.includes(constants.TABLE_SUBS_CHANNEL)) return

        // Register event handler.
        pgListener.notifications.on(
            constants.TABLE_SUBS_CHANNEL,
            event => this._onTableDataChange(event),
        )

        // Actually start listening to table data changes.
        try {
            await pgListener.connect()
            await pgListener.listenTo(constants.TABLE_SUBS_CHANNEL)
        } catch (err) {
            logger.error(`Error connecting to table-subs notification channel: ${err}`)
        }
    }

    _onTableDataChange(event: TableSubEvent) {
        if (!event) return
        const { schema, table } = event
        const tablePath = [schema, table].join('.')

        // Get table sub this event belongs to.
        const tableSub = this.tableSubs[tablePath]
        if (!tableSub || tableSub.status !== TableSubStatus.Subscribed) {
            logger.warn(`Got data-change event for table (${tablePath}) not subscribed to...`)
            return
        }

        // Ensure event record isn't blacklisted.
        if (tableSub.blacklist.has(this._primaryKeysToBlacklistKey(event.primaryKeys))) {
            return
        }
        
        // Immediately process events if buffer hits max capacity.
        this.tableSubs[tablePath].buffer.push(event)
        if (this.tableSubs[tablePath].buffer.length >= constants.TABLE_SUB_BUFFER_MAX_SIZE) {
            this.tableSubs[tablePath].processEvents.flush()
            return
        }

        // Debounce.
        this.tableSubs[tablePath].processEvents()
    }

    _primaryKeysToBlacklistKey(primaryKeys: StringKeyMap) {
        return Object.keys(primaryKeys).sort().map(k => primaryKeys[k]).join(':')
    }

    _blacklistRecords(tablePath: string, events: TableSubEvent[]): string[] {
        const keysAdded = []
        events.forEach(event => {
            const key = this._primaryKeysToBlacklistKey(event.primaryKeys)
            this.tableSubs[tablePath].blacklist.add(key)
            keysAdded.push(key)
        })
        return keysAdded
    }

    _unblacklistRecords(tablePath: string, keys: string[]) {
        keys.forEach(key => {
            this.tableSubs[tablePath].blacklist.delete(key)
        })
    }
    
    async _processTableSubEvents(tablePath: string) {
        // Get table sub for path.
        const tableSub = this.tableSubs[tablePath]
        if (!tableSub) return
        
        // Extract events from buffer / reset buffer.
        const events = [...tableSub.buffer]
        this.tableSubs[tablePath].buffer = []

        logger.info(`Processing ${events.length} data-change event(s) on ${tablePath}...`)

        // Blacklist table events for the records associated with these events.
        const blacklistKeys = this._blacklistRecords(tablePath, events)

        // Populate any live columns on these records.
        try {
            await this._resolveInternalRecords(tableSub, tablePath, events)
        } catch (err) {
            logger.error(err)
        } finally {
            // Unblacklist records now that records are fully resolved.
            this._unblacklistRecords(tablePath, blacklistKeys)
        }

        // Populate other tables dependent on these records for live data.
        await this._resolveExternalRecords(tableSub, tablePath, events)
    }

    async _resolveInternalRecords(tableSub: TableSub, tablePath: string, events: TableSubEvent[]) {
        for (const changes of this._getInternalTableLinkDataChanges(tablePath, events)) {
            await this._processInternalTableLinkDataChanges(tableSub, changes)
        }
    }

    async _resolveExternalRecords(tableSub: TableSub, tablePath: string, events: TableSubEvent[]) {
        await Promise.all(this._getExternalTableLinkDataChanges(tablePath, events).map(changes => 
            this._processExternalTableLinkDataChanges(tableSub, changes)
        ))
    }

    async _processInternalTableLinkDataChanges(tableSub: TableSub, tableLinkDataChanges: TableLinkDataChanges) {
        const { tableLink, events } = tableLinkDataChanges
        const { liveObjectId, link } = tableLink
        const tablePath = [tableSub.schema, tableSub.table].join('.')
        
        // Parse/update primary keys in case of numerics.
        const primaryKeyData = events.map(e => this._parsePrimaryKeys(e.primaryKeys, tableSub.primaryKeyTypes))
        
        // Get full live object associated with table link.
        const liveObject = this.getLiveObject(liveObjectId)
        if (!liveObject) {
            logger.error(`No live object currently registered for: ${liveObjectId}.`)
            return
        }

        // Auto fetch and populate in any live columns on these records.
        try {
            await new ResolveRecordsService(tablePath, liveObject, link, primaryKeyData).perform()
        } catch (err) {
            logger.error(`Failed to auto-resolve live columns on records in ${link.table}: ${err}`)
        }
    }

    async _processExternalTableLinkDataChanges(tableSub: TableSub, tableLinkDataChanges: TableLinkDataChanges) {
        const { tableLink, events } = tableLinkDataChanges
        const { liveObjectId, link } = tableLink
        const tablePath = [tableSub.schema, tableSub.table].join('.')

        // Parse/update primary keys in case of numerics.
        const primaryKeyData = events.map(e => this._parsePrimaryKeys(e.primaryKeys, tableSub.primaryKeyTypes))
        
        // Get all records for primary keys.
        let records: StringKeyMap[]
        try {
            records = await this._getRecordsForPrimaryKeys(tablePath, primaryKeyData)
        } catch (err) {
            logger.error(err)
            return
        }

        // Get full live object associated with table link.
        const liveObject = this.getLiveObject(liveObjectId)
        if (!liveObject) {
            logger.error(`No live object currently registered for: ${liveObjectId}.`)
            return
        }

        // Auto-seed this link's table using the given foreign records as batch input.
        const seedSpec = {
            liveObjectId,
            tablePath: link.table,
            linkProperties: link.properties,
            seedWith: link.seedWith,
            uniqueBy: link.uniqueBy,
            seedColNames: [], // not used with foreign seeds
            seedIfEmpty: link.seedIfEmpty || false,
        }
        try {
            await new SeedTableService(seedSpec, liveObject).seedWithForeignRecords(tablePath, records)
        } catch (err) {
            logger.error(`Failed to auto-seed ${link.table} using ${tablePath}: ${err}`)
        }
    }

    _getInternalTableLinkDataChanges(tablePath: string, events: TableSubEvent[]): TableLinkDataChanges[] {
        // Get all links where this table is the target.
        const tableLinks = config.getLinksForTable(tablePath)

        const internalLinksToProcess = []
        for (const tableLink of tableLinks) {
            const linkColPaths = Object.values(tableLink.link.properties)
            
            const eventsAffectingLinkedCols = []
            for (const event of events) {
                const colNamesAffected = new Set<string>(this._getColNamesAffectedByEvent(event))

                const resolvedLinkColNames = []
                for (const colPath of linkColPaths) {
                    const [colSchema, colTable, colName] = colPath.split('.')
                    const colTablePath = [colSchema, colTable].join('.')
                    let resolvedColNamae = colName

                    if (colTablePath !== tablePath) {
                        const foreignKeyConstraint = getRel(tablePath, colTablePath)
                        if (!foreignKeyConstraint) continue
                        resolvedColNamae = foreignKeyConstraint.foreignKey
                    }

                    resolvedLinkColNames.push(resolvedColNamae)        
                }

                for (const colName of resolvedLinkColNames) {
                    if (colNamesAffected.has(colName)) {
                        eventsAffectingLinkedCols.push(event)
                        break
                    }
                }
            }

            if (!eventsAffectingLinkedCols.length) continue

            internalLinksToProcess.push({
                tableLink: tableLink,
                events: eventsAffectingLinkedCols,
            })
        }

        return internalLinksToProcess
    }

    _getExternalTableLinkDataChanges(tablePath: string, events: TableSubEvent[]): TableLinkDataChanges[] {
        // Get all links where this table is NOT the target BUT IS USED to seed another table.
        const depTableLinks = config.getExternalTableLinksDependentOnTableForSeed(tablePath)

        const externalLinksToProcess = []
        for (const depTableLink of depTableLinks) {            
            const insertsCausingDownstreamSeeds = []
            for (const event of events) {
                // We only care about inserts for dependent table reactions.
                if (event.operation !== TriggerEvent.INSERT) continue

                const colPathsWithValues = new Set<string>((event.colNamesWithValues || []).map(colName => (
                    [tablePath, colName].join('.')
                )))
    
                for (const seedWithProperty of depTableLink.link.seedWith) {
                    const seedWithColPath = depTableLink.link.properties[seedWithProperty]
                    if (!seedWithColPath) continue
                    if (colPathsWithValues.has(seedWithColPath)) {
                        insertsCausingDownstreamSeeds.push(event)
                        break
                    }
                }
            }

            externalLinksToProcess.push({
                tableLink: depTableLink,
                events: insertsCausingDownstreamSeeds,
            })
        }

        return externalLinksToProcess
    }

    async _getRecordsForPrimaryKeys(tablePath: string, primaryKeyData: StringKeyMap[]): Promise<StringKeyMap[]> {
        // Group primary keys into arrays of values for the same key.
        const primaryKeys = {}
        Object.keys(primaryKeyData[0]).forEach(key => {
            primaryKeys[key] = []
        })
        for (const pkData of primaryKeyData) {
            for (const key in pkData) {
                const val = pkData[key]
                primaryKeys[key].push(val)
            }
        }

        // Build query for all records associated with the array of primary keys.
        let query = db.from(tablePath).select('*')
        for (const key in primaryKeys) {
            query.whereIn(key, primaryKeys[key])
        }
        query.limit(primaryKeyData.length)

        try {
            return await query
        } catch (err) {
            const [schema, table] = tablePath.split('.')
            throw new QueryError('select', schema, table, err)
        }
    }

    _getColNamesAffectedByEvent(event: TableSubEvent): string[] {
        switch (event.operation) {
            case TriggerEvent.INSERT:
                return event.colNamesWithValues || []
            case TriggerEvent.UPDATE:
                return event.colNamesChanged || []
            default:
                return []
        }
    }

    _parsePrimaryKeys(primaryKeysData: StringKeyMap, primaryKeyTypes: StringMap): StringKeyMap {
        const newPrimaryKeys = {}
        for (const key in primaryKeysData) {
            let val = primaryKeysData[key]
            const colType = primaryKeyTypes[key]
            if (colType === 'number') {
                val = Number(val)
            }
            newPrimaryKeys[key] = val
        }
        return newPrimaryKeys
     }

    async _upsertTableSub(tableSub: TableSub, existingTriggersMap: { [key: string]: Trigger }) {
        const { schema, table } = tableSub
        const tablePath = [schema, table].join('.')
        const insertTriggerKey = [schema, table, TriggerEvent.INSERT].join(':')
        const updateTriggerKey = [schema, table, TriggerEvent.UPDATE].join(':')

        // Get the current insert & update triggers for this table.
        const insertTrigger = existingTriggersMap[insertTriggerKey]
        const updateTrigger = existingTriggersMap[updateTriggerKey]

        // Should create new triggers if they don't exist.
        let createUpdateTrigger = !updateTrigger
        let createInsertTrigger = !insertTrigger
        
        const currentPrimaryKeyCols = tablesMeta[tablePath].primaryKey
        const currentPrimaryKeys = currentPrimaryKeyCols.map(pk => pk.name)

        // If the insert trigger already exists, ensure the primary keys for the table haven't changed.
        if (insertTrigger) {
            // If the trigger name is different, it means the table's primary keys must have changed,
            // so drop the existing triggers and we'll recreate them (+ the functions).
            const expectedTriggerName = formatTriggerName(schema, table, insertTrigger.event, currentPrimaryKeys)
            if (expectedTriggerName && (insertTrigger.name !== expectedTriggerName)) {
                try {
                    await dropTrigger(insertTrigger)
                    createInsertTrigger = true
                } catch (err) {
                    logger.error(`Error dropping trigger: ${insertTrigger.name}`)
                }
            }
        }

        // If the update trigger already exists, ensure the primary keys for the table haven't changed.
        if (updateTrigger) {
            // If the trigger name is different, it means the table's primary keys must have changed,
            // so drop the existing trigger and we'll recreate it (+ upsert the function).
            const expectedTriggerName = formatTriggerName(schema, table, updateTrigger.event, currentPrimaryKeys)
            if (expectedTriggerName && (updateTrigger.name !== expectedTriggerName)) {
                try {
                    await dropTrigger(updateTrigger)
                    createUpdateTrigger = true
                } catch (err) {
                    logger.error(`Error dropping trigger: ${updateTrigger.name}`)
                }
            }
        }

        // Jump to subscribing status if already created.
        if (!createInsertTrigger && !createUpdateTrigger) {

            // TODO: Consolidate with end of function.
            const primaryKeyTypes = {}
            for (const { name, type } of currentPrimaryKeyCols) {
                primaryKeyTypes[name] = type
            }
    
            this.tableSubs[tablePath].primaryKeyTypes = primaryKeyTypes    
            this.tableSubs[tablePath].status = TableSubStatus.Subscribing
            return
        }

        this.tableSubs[tablePath].status = TableSubStatus.Creating

        // Create the needed triggers.
        const promises = []
        createInsertTrigger && promises.push(createTrigger(schema, table, TriggerEvent.INSERT, { 
            primaryKeys: currentPrimaryKeys,
        }))
        createUpdateTrigger && promises.push(createTrigger(schema, table, TriggerEvent.UPDATE, {
            primaryKeys: currentPrimaryKeys,
        }))

        try {
            await Promise.all(promises)
        } catch (err) {
            logger.error(`Error creating triggers for ${tablePath}: ${err}`)
            return
        }

        const primaryKeyTypes = {}
        for (const { name, type } of currentPrimaryKeyCols) {
            primaryKeyTypes[name] = type
        }

        this.tableSubs[tablePath].primaryKeyTypes = primaryKeyTypes
        this.tableSubs[tablePath].status = TableSubStatus.Subscribing
    }

    _populateTableSubsFromConfig() {
        config.getAllReferencedTablePaths().forEach(tablePath => {
            if (this.tableSubs.hasOwnProperty(tablePath)) return
            const [schema, table] = tablePath.split('.')

            this.tableSubs[tablePath] = {
                status: TableSubStatus.Pending,
                schema,
                table,
                buffer: [],
                processEvents: debounce(
                    () => this._processTableSubEvents(tablePath),
                    constants.TABLE_SUB_BUFFER_INTERVAL,
                ),
                blacklist: new Set<string>(),
            }
        })
    }
}

export const tableSubscriber = new TableSubscriber()