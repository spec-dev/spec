import { pgListener, db, connectionConfig } from '.'
import { getSpecTriggers, formatTriggerName, dropTrigger, createTrigger } from './triggers'
import {
    TableSub,
    TableSubStatus,
    TableSubEvent,
    StringKeyMap,
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
import { filterObjectByKeys, keysWithNonEmptyValues, noop } from '../utils/formatters'
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
import { getHooks, hooksExist } from '../hooks'
import { createRealtimeClient } from '@spec.dev/realtime-client'

let realtimeClient = null
const initRealtimeClient = () => {
    realtimeClient = createRealtimeClient({
        ...connectionConfig,
        channel: constants.TABLE_SUB_CHANNEL,
        bufferInterval: Math.max(constants.TABLE_SUB_BUFFER_INTERVAL, 10),
        maxBufferSize: constants.TABLE_SUB_BUFFER_MAX_SIZE,
        onError: err => logger.error(`Realtime event error: ${err}`)
    })
}

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

        // If Spec was down during any data changes that would have been caught, try to play catch-up.
        await this._detectAndProcessAnyMissedTableSubEvents()

        // Initialize realtime client if hooks exist.
        if (hooksExist() && !realtimeClient) {
            initRealtimeClient()
        }

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

        // Upsert triggers for each table sub.
        await Promise.all(tableSubs.map((ts) => this._upsertTrigger(ts, existingTriggersMap)))
    }

    async deleteTableSub(tablePath: string) {
        delete this.tableSubs[tablePath]
        await deleteTableSubCursor(tablePath)
    }

    async _subscribeToTables(subsThatNeedSubscribing: TableSub[]) {
        const hooks = realtimeClient ? getHooks() : {}

        // Register tables as subscribed.
        for (const { schema, table } of subsThatNeedSubscribing) {
            const tablePath = [schema, table].join('.')
            this.tableSubs[tablePath].status = TableSubStatus.Subscribed
            realtimeClient && this._applyHooksToTable(hooks, schema, table)
            logger.info(`Listening for changes on table ${tablePath}...`)
        }

        // Ensure we're not already subscribed to the table subs channel.
        const subscribedChannels = pgListener.getSubscribedChannels()
        if (subscribedChannels.includes(constants.TABLE_SUB_CHANNEL)) return

        // Register event handler.
        pgListener.notifications.on(constants.TABLE_SUB_CHANNEL, (event) =>
            this._onTableDataChange(event)
        )

        // Actually start listening to table data changes.
        try {
            await pgListener.connect()
            await pgListener.listenTo(constants.TABLE_SUB_CHANNEL)
        } catch (err) {
            logger.error(`Error connecting to table-subs notification channel: ${err}`)
        }
        realtimeClient && await realtimeClient.listen()
    }

    _applyHooksToTable(hooks, schema, table) {
        for (const key in hooks) {
            const parsedKey = this._parseHookEventKey(key) 
            if (parsedKey.schema !== schema || parsedKey.table !== table) {
                continue
            }
            const callback = hooks[key] || noop
            realtimeClient.table(table, { schema }).on(parsedKey.event, events => {
                callback && callback(events).catch(err => logger.error(
                    `Hook error for ${parsedKey.event} event on ${schema}.${table}: ${err}`
                ))
            })
        }
    }

    _parseHookEventKey(key: string): { schema: string, table: string, event: string } {
        const colonSplit = (key || '').split(':')
        if (colonSplit.length !== 2) {
            return { schema: '', table: '', event: '' }
        }
        const [tablePath, event] = colonSplit
        const dotSplit = (tablePath || '').split('.')
        if (dotSplit.length !== 2) {
            return { schema: '', table: '', event: '' }
        }
        const [schema, table] = dotSplit
        return { schema, table, event }        
    }

    _onTableDataChange(event: TableSubEvent) {
        if (!event || event.schema === 'spec' || event?.operation === TriggerEvent.DELETE) {
            return
        }

        // Strip quotes just in case.
        event.table = event.table.replace(/"/g, '')
        const { schema, table } = event
        const tablePath = [schema, table].join('.')

        // Get table sub this event belongs to.
        const tableSub = this.tableSubs[tablePath]
        if (!tableSub || tableSub.status !== TableSubStatus.Subscribed) {
            logger.warn(`Got data-change event for table (${tablePath}) not subscribed to...`)
            return
        }

        // Resolve primary key data from event (might need to be inferred).
        const [primaryKeyData, validatePrimaryKeysMatchTableSub] = this._resolveEventPrimaryKeyData(
            tablePath,
            event
        )
        if (!primaryKeyData) {
            logger.error(`Failed to parse primary key data from event for table ${tablePath}.`)
            return
        }
        event.primaryKeyData = primaryKeyData

        // Make sure the primary keys for this table haven't changed.
        if (
            validatePrimaryKeysMatchTableSub &&
            !this._doPrimaryKeysMatchTableSub(primaryKeyData, tablePath)
        ) {
            return
        }

        // Ensure event record isn't blacklisted.
        if (tableSub.blacklist.has(this._primaryKeysToBlacklistKey(primaryKeyData))) {
            return
        }

        // Always add to buffer first.
        this.tableSubs[tablePath].buffer.push(event)

        // Immediately process events if buffer hits max capacity.
        if (this.tableSubs[tablePath].buffer.length >= constants.TABLE_SUB_BUFFER_MAX_SIZE) {
            this.tableSubs[tablePath].processEvents.flush()
            return
        }

        // Debounce.
        this.tableSubs[tablePath].processEvents()
    }

    _resolveEventPrimaryKeyData(
        tablePath: string,
        event: TableSubEvent
    ): [StringKeyMap | null, boolean] {
        switch (event.operation) {
            case TriggerEvent.INSERT:
            case TriggerEvent.UPDATE:
                const eventHasPrimaryKeyData = event.hasOwnProperty('primaryKeyData')
                const primaryKeyData = eventHasPrimaryKeyData
                    ? event.primaryKeyData
                    : this._parsePrimaryKeyDataFromRecord(tablePath, event.data)
                return [primaryKeyData, eventHasPrimaryKeyData]

            case TriggerEvent.MISSED:
                return [this._parsePrimaryKeyDataFromRecord(tablePath, event.data), false]

            default:
                return [null, false]
        }
    }

    _parsePrimaryKeyDataFromRecord(tablePath: string, data: StringKeyMap): StringKeyMap | null {
        if (!data) {
            logger.error(
                `${tablePath} - Can't parse primary key data from event with no data itself...`
            )
            return null
        }
        const tableMeta = tablesMeta[tablePath]
        if (!tableMeta) {
            logger.error(`No meta registered for table ${tablePath}`)
            return null
        }
        return filterObjectByKeys(
            data,
            tableMeta.primaryKey.map((pk) => pk.name)
        )
    }

    _doPrimaryKeysMatchTableSub(eventPrimaryKeys: StringKeyMap, tablePath: string): boolean {
        const tableMeta = tablesMeta[tablePath]
        if (!tableMeta) {
            logger.error(`No meta registered for table ${tablePath}`)
            return false
        }

        const eventPrimaryKeysId = Object.keys(eventPrimaryKeys).sort().join(',')
        const primaryKeyId = tableMeta.primaryKey
            .map((pk) => pk.name)
            .sort()
            .join(',')

        if (eventPrimaryKeysId !== primaryKeyId) {
            logger.error(
                "Primary key columns given in data-change event don't\n" +
                    `match the current primary keys for the table ${tablePath}:\n` +
                    `${eventPrimaryKeysId} <> ${primaryKeyId}`
            )
            return false
        }

        return true
    }

    _primaryKeysToBlacklistKey(primaryKeyData: StringKeyMap) {
        return Object.keys(primaryKeyData)
            .sort()
            .map((k) => primaryKeyData[k])
            .join(':')
    }

    _blacklistRecords(tablePath: string, events: TableSubEvent[]): string[] {
        const keysAdded = []
        events.forEach((event) => {
            const key = this._primaryKeysToBlacklistKey(event.primaryKeyData)
            this.tableSubs[tablePath].blacklist.add(key)
            keysAdded.push(key)
        })
        return keysAdded
    }

    _unblacklistRecords(tablePath: string, keys: string[]) {
        keys.forEach((key) => this.tableSubs[tablePath].blacklist.delete(key))
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
            const primaryKeyData = {}
            for (const key of primaryKeyColNames) {
                primaryKeyData[key] = record[key]
            }
            this._onTableDataChange({
                timestamp: '', // doesn't matter
                operation: TriggerEvent.MISSED,
                schema,
                table,
                primaryKeyData,
                data: record,
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
            // TODO: Not using ResolveRecordsService until you make decision about how to handle seeds that add new records with nullable values in nullable columns.
            // await this._processInternalTableLinkDataChanges(tableSub, changes)
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
            primaryKeyData: events.map((e) => e.primaryKeyData),
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

        // Split events up by those that already have full records (event.data)
        // and those that don't (need to leverage event.primaryKeyData).
        const records = []
        const resolveRecordsWithPrimaryKeys = []
        const primaryKeyDataForAllEvents = []
        for (const event of events) {
            primaryKeyDataForAllEvents.push(event.primaryKeyData)
            if (event.data) {
                records.push(event.data)
                continue
            }
            resolveRecordsWithPrimaryKeys.push(event.primaryKeyData)
        }

        // If only processing missed events, we already have the records.
        if (resolveRecordsWithPrimaryKeys.length) {
            try {
                const resolvedRecords = await getRecordsForPrimaryKeys(
                    tablePath,
                    resolveRecordsWithPrimaryKeys
                )
                records.push(...(resolvedRecords || []))
            } catch (err) {
                logger.error(`Error resolving records by primary keys: ${err}`)
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
            seedColNames: [], // not used with foreign seeds
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
                foreignPrimaryKeyData: primaryKeyDataForAllEvents,
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
            return
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
            const linkColPaths = Object.values(tableLink.enrichedLink.linkOn)

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
            const filterColPaths = depTableLink.filterColPaths || []
            if (!filterColPaths.length) continue

            const eventsCausingDownstreamSeeds = []
            for (const event of events) {
                const affectedColNames = this._getColNamesAffectedByEvent(event)
                const affectedColPaths = new Set<string>(
                    affectedColNames.map((colName) => [tablePath, colName].join('.'))
                )

                for (const colPath of filterColPaths) {
                    if (affectedColPaths.has(colPath)) {
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
                return event.hasOwnProperty('nonEmptyColumns')
                    ? event.nonEmptyColumns || []
                    : keysWithNonEmptyValues(event.data)

            case TriggerEvent.UPDATE:
                return event.columnNamesChanged || []

            case TriggerEvent.MISSED:
                return keysWithNonEmptyValues(event.data)

            default:
                return []
        }
    }

    async _upsertTrigger(tableSub: TableSub, existingTriggersMap: { [key: string]: Trigger }) {
        const { schema, table } = tableSub
        const tablePath = [schema, table].join('.')
        const currentPrimaryKeyCols = tablesMeta[tablePath].primaryKey
        const currentPrimaryKeys = currentPrimaryKeyCols.map((pk) => pk.name)

        // Get the current spec triggers for this table.
        const insertTriggerKey = [schema, table, TriggerEvent.INSERT].join(':')
        const updateTriggerKey = [schema, table, TriggerEvent.UPDATE].join(':')
        const deleteTriggerKey = [schema, table, TriggerEvent.DELETE].join(':')
        const insertTrigger = existingTriggersMap[insertTriggerKey]
        const updateTrigger = existingTriggersMap[updateTriggerKey]
        const deleteTrigger = existingTriggersMap[deleteTriggerKey]

        // Should create new triggers if they don't exist.
        let createInsertTrigger = !insertTrigger
        let createUpdateTrigger = !updateTrigger
        let createDeleteTrigger = !deleteTrigger

        // If any of the triggers already exist, ensure the primary keys haven't changed.
        if (insertTrigger) {
            createInsertTrigger = await this._maybeDropTrigger(
                insertTrigger,
                schema,
                table,
                currentPrimaryKeys
            )
        }
        if (updateTrigger) {
            createUpdateTrigger = await this._maybeDropTrigger(
                updateTrigger,
                schema,
                table,
                currentPrimaryKeys
            )
        }
        if (deleteTrigger) {
            createDeleteTrigger = await this._maybeDropTrigger(
                deleteTrigger,
                schema,
                table,
                currentPrimaryKeys
            )
        }

        // Jump to subscribing status and end early if already created all triggers.
        if (!createInsertTrigger && !createUpdateTrigger && !createDeleteTrigger) {
            this.tableSubs[tablePath].status = TableSubStatus.Subscribing
            return
        }

        this.tableSubs[tablePath].status = TableSubStatus.Creating

        // Create the missing triggers.
        const promises = []
        createInsertTrigger && promises.push(createTrigger(schema, table, TriggerEvent.INSERT))
        createUpdateTrigger && promises.push(createTrigger(schema, table, TriggerEvent.UPDATE))
        createUpdateTrigger && promises.push(createTrigger(schema, table, TriggerEvent.DELETE))
        try {
            await Promise.all(promises)
        } catch (err) {
            logger.error(`Error creating triggers for ${tablePath}: ${err}`)
            return
        }

        this.tableSubs[tablePath].status = TableSubStatus.Subscribing
    }

    async _maybeDropTrigger(
        trigger: Trigger,
        schema: string,
        table: string,
        primaryKeyColumnNames: string[]
    ): Promise<boolean> {
        let shouldCreateNewTrigger = false

        // If the trigger name is different, it means the table's primary keys must have changed,
        // so drop the existing trigger and we'll recreate it (+ upsert the function).
        const expectedTriggerName = formatTriggerName(
            schema,
            table,
            trigger.event,
            primaryKeyColumnNames
        )
        if (expectedTriggerName && trigger.name !== expectedTriggerName) {
            try {
                // May fail unless spec is given enhanced permissions.
                await dropTrigger(trigger)
                shouldCreateNewTrigger = true
            } catch (err) {
                logger.error(`Error dropping trigger: ${trigger.name}`)
                shouldCreateNewTrigger = false
            }
        }

        return shouldCreateNewTrigger
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
