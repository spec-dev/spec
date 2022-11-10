import { pgListener, db } from '.'
import { getSpecTriggers, formatTriggerName, dropTrigger, createTrigger } from './triggers'
import {
    TableSub,
    TableSubStatus,
    TableSubEvent,
    StringKeyMap,
    StringMap,
    Trigger,
    TriggerEvent,
    TableLinkDataChanges,
    LiveObject,
    TableSubCursor,
    ResolveRecordsSpec,
    SeedCursorJobType,
    SeedCursorStatus,
} from '../types'
import { tablesMeta, getRel } from './tablesMeta'
import config from '../config'
import logger from '../logger'
import SeedTableService from '../services/SeedTableService'
import ResolveRecordsService from '../services/ResolveRecordsService'
import constants from '../constants'
import { debounce } from 'lodash'
import { getRecordsForPrimaryKeys } from './ops'
import {
    getTableSubCursorsForPaths,
    upsertTableSubCursor,
    deleteTableSubCursor,
} from './spec/tableSubCursors'
import { createSeedCursor, seedFailed, seedSucceeded } from './spec/seedCursors'
import short from 'short-uuid'

export class TableSubscriber {
    tableSubs: { [key: string]: TableSub } = {}

    // Set dynamically from the Spec class.
    getLiveObject: (id: string) => LiveObject

    async upsertTableSubs() {
        // Create a map of table subs from the tables mentioned in the config.
        this._populateTableSubsFromConfig()

        // Find which table subs are pending (which ones we need to check on to make sure they exist).
        const pendingSubs = Object.values(this.tableSubs).filter(
            (ts) => ts.status === TableSubStatus.Pending
        )
        if (!pendingSubs.length) return

        // Upsert all pending subs (with their Postgres triggers and functions).
        await this.upsertTableSubsWithTriggers(pendingSubs)

        // All subs that were successfully upserted in the previous step should now be in the subscribing state.
        const subsThatNeedSubscribing = Object.values(this.tableSubs).filter(
            (ts) => ts.status === TableSubStatus.Subscribing
        )
        if (!subsThatNeedSubscribing.length) {
            logger.warn('Not all pending table subs moved to the subscribing status...')
            return
        }

        // If Spec was down during any data changes that would have been caught, play catch-up.
        await this._detectAndProcessAnyMissedTableSubEvents()

        // Subscribe to tables (listen for trigger notifications).
        await this._subscribeToTables(subsThatNeedSubscribing)
    }

    async upsertTableSubsWithTriggers(tableSubs: TableSub[]) {
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

        // Upsert table subs with their Postgres triggers and functions.
        await Promise.all(tableSubs.map((ts) => this._upsertTableSub(ts, existingTriggersMap)))
    }

    async deleteTableSub(tablePath: string) {
        delete this.tableSubs[tablePath]
        await deleteTableSubCursor(tablePath)
    }

    async _subscribeToTables(subsThatNeedSubscribing: TableSub[]) {
        // Register tables as subscribed.
        for (const { schema, table } of subsThatNeedSubscribing) {
            const tablePath = [schema, table].join('.')
            this.tableSubs[tablePath].status = TableSubStatus.Subscribed
            logger.info(`Listening for changes on ${tablePath}...`)
        }

        // Ensure we're not already subscribed to the table subs channel.
        const subscribedChannels = pgListener.getSubscribedChannels()
        if (subscribedChannels.includes(constants.TABLE_SUBS_CHANNEL)) return

        // Register event handler.
        pgListener.notifications.on(constants.TABLE_SUBS_CHANNEL, (event) =>
            this._onTableDataChange(event)
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
        event.table = event.table.replace(/"/g, '')
        const { schema, table } = event
        const tablePath = [schema, table].join('.')

        // Get table sub this event belongs to.
        const tableSub = this.tableSubs[tablePath]
        if (!tableSub || tableSub.status !== TableSubStatus.Subscribed) {
            logger.warn(`Got data-change event for table (${tablePath}) not subscribed to...`)
            return
        }

        // Make sure the primary keys for this table haven't changed....
        if (!this._ensurePrimaryKeysMatchTableSub(event.primaryKeys, tablePath)) {
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

    _ensurePrimaryKeysMatchTableSub(eventPrimaryKeys: StringKeyMap, tablePath: string): boolean {
        const tableMeta = tablesMeta[tablePath]
        if (!tableMeta) {
            logger.error(`No meta registered for table ${tablePath}`)
            return
        }

        const eventPrimaryKeysId = Object.keys(eventPrimaryKeys).sort().join(',')
        const primaryKeyId = tableMeta.primaryKey
            .map((pk) => pk.name)
            .sort()
            .join(',')

        if (eventPrimaryKeysId !== primaryKeyId) {
            logger.error(
                `Primary key columns given in data-change event don't 
                match the current primary keys for the table ${tablePath}: 
                ${eventPrimaryKeysId} <> ${primaryKeyId}`
            )
            return false
        }

        return true
    }

    _primaryKeysToBlacklistKey(primaryKeys: StringKeyMap) {
        return Object.keys(primaryKeys)
            .sort()
            .map((k) => primaryKeys[k])
            .join(':')
    }

    _blacklistRecords(tablePath: string, events: TableSubEvent[]): string[] {
        const keysAdded = []
        events.forEach((event) => {
            const key = this._primaryKeysToBlacklistKey(event.primaryKeys)
            this.tableSubs[tablePath].blacklist.add(key)
            keysAdded.push(key)
        })
        return keysAdded
    }

    _unblacklistRecords(tablePath: string, keys: string[]) {
        keys.forEach((key) => {
            this.tableSubs[tablePath].blacklist.delete(key)
        })
    }

    async _detectAndProcessAnyMissedTableSubEvents() {
        // Get table paths referenced in the config file that have an 'updated_at' timestamp column.
        const tablePathsWithUpdatedAtCols = config.getAllReferencedTablePathsTrackingRecordUpdates()
        if (!tablePathsWithUpdatedAtCols.length) return

        // Get the associated table_sub_cursors (if any).
        const tableSubCursors = await getTableSubCursorsForPaths(tablePathsWithUpdatedAtCols)
        if (!tableSubCursors.length) return

        // Process any missed events for these tables in parallel.
        tableSubCursors.forEach((t) => this._processMissedEventsForTableSubCursor(t))
    }

    async _processMissedEventsForTableSubCursor(tableSubCursor: TableSubCursor) {
        const { tablePath, timestamp } = tableSubCursor
        const [schema, table] = tablePath.split('.')
        const meta = tablesMeta[tablePath]
        if (!meta) return
        const primaryKeyColNames = meta.primaryKey.map((pk) => pk.name)

        // Find records that were updated after the last registered cursor for this table.
        let recordsUpdatedAfterLastCursor = []
        try {
            recordsUpdatedAfterLastCursor = await db
                .from(tablePath)
                .select('*')
                .where(
                    db.raw(`timezone('UTC', ${constants.TABLE_SUB_UPDATED_AT_COL_NAME}) > ?`, [
                        timestamp.toISOString(),
                    ])
                )
        } catch (err) {
            logger.error(
                `Error finding missed events for table sub cursor for ${tablePath}: ${err}`
            )
            return
        }
        if (!recordsUpdatedAfterLastCursor?.length) return

        // Register missed table data-change events for each of these records.
        recordsUpdatedAfterLastCursor.forEach((record) => {
            const primaryKeys = {}
            for (const key of primaryKeyColNames) {
                primaryKeys[key] = record[key]
            }

            this._onTableDataChange({
                timestamp: '', // doesn't matter
                operation: TriggerEvent.MISSED,
                schema,
                table,
                primaryKeys,
                record,
            })
        })
    }

    async _processTableSubEvents(tablePath: string) {
        // Get table sub for path.
        const tableSub = this.tableSubs[tablePath]
        if (!tableSub) return

        // Extract events from buffer / reset buffer.
        const events = [...tableSub.buffer]
        this.tableSubs[tablePath].buffer = []

        // Mark new timestamp for table sub cursor.
        upsertTableSubCursor(tablePath)

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
        await Promise.all(
            this._getExternalTableLinkDataChanges(tablePath, events).map((changes) =>
                this._processExternalTableLinkDataChanges(tableSub, changes)
            )
        )
    }

    async _processInternalTableLinkDataChanges(
        tableSub: TableSub,
        tableLinkDataChanges: TableLinkDataChanges
    ) {
        const { tableLink, events } = tableLinkDataChanges
        const { liveObjectId, link } = tableLink
        const tablePath = [tableSub.schema, tableSub.table].join('.')

        // Parse/update primary keys in case of numerics.
        const primaryKeyData = events.map((e) =>
            this._parsePrimaryKeys(e.primaryKeys, tableSub.primaryKeyTypes)
        )

        // Get full live object associated with table link.
        const liveObject = this.getLiveObject(liveObjectId)
        if (!liveObject) {
            logger.error(`No live object currently registered for: ${liveObjectId}.`)
            return
        }

        // Create resolve records spec.
        const resolveRecordsSpec: ResolveRecordsSpec = {
            liveObjectId,
            tablePath,
            primaryKeyData,
        }

        // Create and save seed cursor.
        const seedCursor = {
            id: short.generate(),
            jobType: SeedCursorJobType.ResolveRecords,
            spec: resolveRecordsSpec,
            status: SeedCursorStatus.InProgress,
            cursor: 0,
        }
        await createSeedCursor(seedCursor)

        // Auto-resolve and populate any live columns on these records.
        try {
            const resolveRecordsService = new ResolveRecordsService(
                resolveRecordsSpec,
                liveObject,
                link,
                seedCursor.id,
                seedCursor.cursor
            )
            await resolveRecordsService.perform()
        } catch (err) {
            logger.error(`Failed to auto-resolve live columns on records in ${link.table}: ${err}`)
            await seedFailed(seedCursor.id)
        }

        // Register seed cursor as successful.
        await seedSucceeded(seedCursor.id)
    }

    async _processExternalTableLinkDataChanges(
        tableSub: TableSub,
        tableLinkDataChanges: TableLinkDataChanges
    ) {
        const { tableLink, events } = tableLinkDataChanges
        const { liveObjectId, link } = tableLink
        const tablePath = [tableSub.schema, tableSub.table].join('.')

        // Parse/update primary keys in case of numerics.
        const primaryKeyData = events.map((e) =>
            this._parsePrimaryKeys(e.primaryKeys, tableSub.primaryKeyTypes)
        )

        // Get all records for primary keys.
        let records: StringKeyMap[]

        // Check if all events in this batch are missed events.
        let allEventsAreMissedEvents = true
        let missedEventRecords = []
        for (const event of events) {
            if (event.operation !== TriggerEvent.MISSED) {
                allEventsAreMissedEvents = false
                break
            }
            missedEventRecords.push(event.record)
        }

        // If only processing missed events, we already have the records.
        if (allEventsAreMissedEvents) {
            records = missedEventRecords
        } else {
            try {
                records = await getRecordsForPrimaryKeys(tablePath, primaryKeyData)
            } catch (err) {
                logger.error(err)
                return
            }
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
            linkProperties: link.linkOn,
            seedWith: link.seedWith,
            uniqueBy: link.uniqueBy || null,
            filterBy: link.filterBy || null,
            seedColNames: [], // not used with foreign seeds
            seedIfEmpty: link.seedIfEmpty || false,
        }

        // Create and save seed cursor.
        const seedCursor = {
            id: short.generate(),
            jobType: SeedCursorJobType.SeedTable,
            spec: seedSpec,
            status: SeedCursorStatus.InProgress,
            cursor: 0,
            metadata: {
                foreignTablePath: tablePath,
                foreignPrimaryKeyData: primaryKeyData,
            },
        }
        await createSeedCursor(seedCursor)

        // Seed table using foreign records.
        try {
            const seedTableService = new SeedTableService(
                seedSpec,
                liveObject,
                seedCursor.id,
                seedCursor.cursor
            )
            await seedTableService.seedWithForeignRecords(tablePath, records)
        } catch (err) {
            logger.error(`Failed to auto-seed ${link.table} using ${tablePath}: ${err}`)
            await seedFailed(seedCursor.id)
        }

        // Register seed cursor as successful.
        await seedSucceeded(seedCursor.id)
    }

    _getInternalTableLinkDataChanges(
        tablePath: string,
        events: TableSubEvent[]
    ): TableLinkDataChanges[] {
        // Get all links where this table is the target.
        const tableLinks = config.getLinksForTable(tablePath)
        if (!tableLinks.length) return []

        // Get names of all live columns in the table.
        const [schema, table] = tablePath.split('.')
        const liveColumnNames = Object.keys(config.getTable(schema, table) || {}) || []

        const internalLinksToProcess = []
        for (const tableLink of tableLinks) {
            const linkColPaths = Object.values(tableLink.link.linkOn)

            const eventsAffectingLinkedCols = []
            for (const event of events) {
                const colNamesAffected = new Set<string>(this._getColNamesAffectedByEvent(event))

                // There's no need to "resolve" records that either...
                // (1) Are newly inserted records with values already assigned to each live column
                // (2) Are updated records where all live columns were updated simultaneously.
                let allLiveColsWereAffectedSimultaneously = true
                for (const liveColName of liveColumnNames) {
                    if (!colNamesAffected.has(liveColName)) {
                        allLiveColsWereAffectedSimultaneously = false
                        break
                    }
                }
                if (
                    allLiveColsWereAffectedSimultaneously &&
                    event.operation !== TriggerEvent.MISSED
                ) {
                    continue
                }

                const resolvedLinkColNames = []
                for (const colPath of linkColPaths) {
                    const [colSchema, colTable, colName] = colPath.split('.')
                    const colTablePath = [colSchema, colTable].join('.')
                    let resolvedColName = colName

                    if (colTablePath !== tablePath) {
                        const foreignRel = getRel(tablePath, colTablePath)
                        if (!foreignRel) continue
                        resolvedColName = foreignRel.foreignKey
                    }

                    resolvedLinkColNames.push(resolvedColName)
                }

                // Process the event if any linked columns were affected.
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

    _getExternalTableLinkDataChanges(
        tablePath: string,
        events: TableSubEvent[]
    ): TableLinkDataChanges[] {
        // Get all links where this table is NOT the target BUT IS USED to seed another table.
        const depTableLinks = config.getExternalTableLinksDependentOnTableForSeed(tablePath)

        const externalLinksToProcess = []
        for (const depTableLink of depTableLinks) {
            const seedColPaths = depTableLink.seedColPaths || []
            if (!seedColPaths.length) continue

            const eventsCausingDownstreamSeeds = []
            for (const event of events) {
                const colNamesWithValues = this._getColNamesAffectedByEvent(event)
                const colPathsWithValues = new Set<string>(
                    colNamesWithValues.map((colName) => [tablePath, colName].join('.'))
                )

                for (const colPath of seedColPaths) {
                    if (colPathsWithValues.has(colPath)) {
                        eventsCausingDownstreamSeeds.push(event)
                        break
                    }
                }
            }

            externalLinksToProcess.push({
                tableLink: depTableLink,
                events: eventsCausingDownstreamSeeds,
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
            case TriggerEvent.MISSED:
                const colNamesWithValues = []
                for (const colName in event.record) {
                    const val = event.record[colName]
                    if (val !== null) {
                        colNamesWithValues.push(colName)
                    }
                }
                return colNamesWithValues
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
        const currentPrimaryKeys = currentPrimaryKeyCols.map((pk) => pk.name)

        // If the insert trigger already exists, ensure the primary keys for the table haven't changed.
        if (insertTrigger) {
            // If the trigger name is different, it means the table's primary keys must have changed,
            // so drop the existing triggers and we'll recreate them (+ the functions).
            const expectedTriggerName = formatTriggerName(
                schema,
                table,
                insertTrigger.event,
                currentPrimaryKeys
            )
            if (expectedTriggerName && insertTrigger.name !== expectedTriggerName) {
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
            const expectedTriggerName = formatTriggerName(
                schema,
                table,
                updateTrigger.event,
                currentPrimaryKeys
            )
            if (expectedTriggerName && updateTrigger.name !== expectedTriggerName) {
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
        createInsertTrigger &&
            promises.push(
                createTrigger(schema, table, TriggerEvent.INSERT, {
                    primaryKeys: currentPrimaryKeys,
                })
            )
        createUpdateTrigger &&
            promises.push(
                createTrigger(schema, table, TriggerEvent.UPDATE, {
                    primaryKeys: currentPrimaryKeys,
                })
            )

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
        config.getAllReferencedTablePaths().forEach((tablePath) => {
            if (this.tableSubs.hasOwnProperty(tablePath)) return
            const [schema, table] = tablePath.split('.')

            this.tableSubs[tablePath] = {
                status: TableSubStatus.Pending,
                schema,
                table,
                buffer: [],
                processEvents: debounce(
                    () => this._processTableSubEvents(tablePath),
                    constants.TABLE_SUB_BUFFER_INTERVAL
                ),
                blacklist: new Set<string>(),
            }
        })
    }
}

export const tableSubscriber = new TableSubscriber()
