
import { pgListener } from '.'
import { getSpecTriggers, formatTriggerName, dropTrigger, createTrigger } from './triggers'
import { TableSub, TableSubStatus, TableSubEvent, StringKeyMap, StringMap, Trigger, TriggerEvent, TableLinkDataChanges } from '../types'
import { tablesMeta, getRel } from './tablesMeta'
import config from '../config'
import logger from '../logger'
import constants from '../constants'
import debounce from 'lodash.debounce'

export class TableSubscriber {

    tableSubs: { [key: string]: TableSub } = {}

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

        // Ensure event meets processing criteria.
        if (!this._shouldProcessEvent(event)) return
        
        // Immediately process events if buffer hits max capacity.
        this.tableSubs[tablePath].buffer.push(event)
        if (this.tableSubs[tablePath].buffer.length >= constants.TABLE_SUB_BUFFER_MAX_SIZE) {
            this.tableSubs[tablePath].processEvents.flush()
            return
        }

        // Debounce.
        this.tableSubs[tablePath].processEvents()
    }

    _shouldProcessEvent(event: TableSubEvent): boolean {
        return true
    }

    async _processTableSubEvents(tablePath: string) {
        // Get table sub for path.
        const tableSub = this.tableSubs[tablePath]
        if (!tableSub) return
        
        // Extract events from buffer / reset buffer.
        const events = [...tableSub.buffer]
        this.tableSubs[tablePath].buffer = []

        // Fill out yourself.
        await this._resolveInternalRecords(tablePath, events)

        // Fill out those dependent on you.
        await this._resolveExternalRecords(tablePath, events)
    }

    async _resolveInternalRecords(tablePath: string, events: TableSubEvent[]) {
        for (const changes of this._getInternalTableLinkDataChanges(tablePath, events)) {
            await this._processInternalTableLinkDataChanges(changes)
        }
    }

    async _resolveExternalRecords(tablePath: string, events: TableSubEvent[]) {
        await Promise.all(this._getExternalTableLinkDataChanges(tablePath, events).map(changes => 
            this._processExternalTableLinkDataChanges(changes)
        ))
    }

    async _processInternalTableLinkDataChanges(tableLinkDataChanges: TableLinkDataChanges) {
        const { tableLink, events } = tableLinkDataChanges

        // Primary keys --> Record Batch --> Function Args --> Edge Function --> Live Objects --> Merge back with Primary Keys --> Update Ops

        
        
        // TODO: Do this later when you actually need it
        // Parse/update primary keys in case of numerics.
        // event.primaryKeys = this._parsePrimaryKeys(event.primaryKeys, tableSub.primaryKeyTypes)
    }

    async _processExternalTableLinkDataChanges(tableLinkDataChanges: TableLinkDataChanges) {
        const { tableLink, events } = tableLinkDataChanges

        // Primary keys --> Record Batch --> Function Args --> Edge Function --> Live Objects --> Merge back with Primary Keys --> Update Ops
        
        // TODO: Do this later when you actually need it
        // Parse/update primary keys in case of numerics.
        // event.primaryKeys = this._parsePrimaryKeys(event.primaryKeys, tableSub.primaryKeyTypes)
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

            internalLinksToProcess.push({
                tableLink: tableLink,
                events: eventsAffectingLinkedCols,
            })
        }

        return internalLinksToProcess
    }

    _getExternalTableLinkDataChanges(tablePath: string, events: TableSubEvent[]): TableLinkDataChanges[] {
        // Get all links where this table is NOT the target BUT 
        // is used to seed another table (i.e. dependent tables).
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
                )
            }
        })
    }
}

export const tableSubscriber = new TableSubscriber()