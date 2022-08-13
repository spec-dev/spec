import logger from './lib/logger'
import config from './lib/config'
import constants from './lib/constants'
import { resolveLiveObjects } from './lib/rpcs/liveObjects'
import messageClient from './lib/rpcs/messageClient'
import { LiveObject, StringKeyMap, EventSub, SeedSpec, TableSub, TableSubStatus, TriggerEvent, Trigger, TableSubEvent, DBColumn, StringMap } from './lib/types'
import { ensureSpecSchemaIsReady, getEventCursorsForNames, saveEventCursors, seedFailed, seedSucceeded } from './lib/db/spec'
import { SpecEvent } from '@spec.dev/event-client'
import { getPrimaryKeys } from './lib/db/ops'
import { tableSubscriber } from './lib/db'
import { formatTriggerName, dropTrigger, createTrigger } from './lib/db/triggers'
import LRU from 'lru-cache'
import ApplyEventService from './lib/services/ApplyEventService'
import UpsertLiveColumnsService from './lib/services/UpsertLiveColumnsService'
import SeedTableService from './lib/services/SeedTableService'
import { getSpecTriggers } from './lib/db/triggers'

class Spec {

    liveObjects: { [key: string]: LiveObject } = {}

    eventSubs: { [key: string]: EventSub } = {}

    saveEventCursorsJob: any = null

    isProcessingNewConfig: boolean = false

    hasPendingConfigUpdate: boolean = false

    liveObjectsToIgnoreEventsFrom: Set<string> = new Set()

    tableSubs: { [key: string]: TableSub } = {}
    
    seenEvents: LRU<string, boolean> = new LRU({
        max: 5000, // TODO: Move to constants and potentially make configurable via env vars
    })
    
    async start() {
        logger.info('Starting Spec...')

        // Run any time the message client socket connects.
        messageClient.onConnect = () => this._onMessageClientConnected()

        // Ensure the 'spec' schema and associated tables within it exist.
        await ensureSpecSchemaIsReady()

        // Subscribe & react to config file changes.
        config.onUpdate = () => {
            if (this.isProcessingNewConfig) {
                this.hasPendingConfigUpdate = true
            } else {
                this._onNewConfig()
            }
        }
        config.watch()

        // Force register an update to kick things off.
        config.onUpdate()
    }

    async _onNewConfig() {
        this.isProcessingNewConfig = true

        // Load and validate the project config file.
        config.load()
        config.validate()

        // If the config is invalid, just wait until the next save to try again.
        if (!config.isValid) {
            this._doneProcessingNewConfig()
            return
        }

        // Upsert table subscriptions (listen to data changes).
        this._upsertTableSubs()

        // Connect to event/rpc message client. 
        // Force run the onConnect handler if already connected.
        messageClient.client ? messageClient.onConnect() : messageClient.connect()
    }

    async _onMessageClientConnected() {
        // Resolve all live objects for the versions listed in the config file.
        const newLiveObjects = await this._getLiveObjectsInConfig()
        if (this.liveObjects === null) {
            logger.info('No live objects listed in config.')
            this._doneProcessingNewConfig()
            return
        }
        
        // Upsert live columns listed in the config and start seeding the new ones.
        await this._upsertAndSeedLiveColumns()

        // Subscribe to all events powering the live objects.
        const newEventNames = this._subscribeToLiveObjectEvents()
        if (!Object.keys(this.eventSubs).length) {
            logger.info('No events to subscribe to.')
            this._doneProcessingNewConfig()
            return
        }

        // Load the latest events received by each sub.
        await this._loadEventCursors()

        // Fetch missed events for any new sub that already has an 
        // existing event cursor (i.e. events that have been seen before).
        await this._fetchMissedEvents(newEventNames)

        // Process all buffered events. Missed events will be processed here 
        // too, since the ones fetched above will be added to the buffer.
        await this._processAllBufferedEvents()

        // Start saving event cursors on an interval (+ save immediately).
        this._saveEventCursors()
        this._createSaveCursorsJob()
        this._doneProcessingNewConfig()
    }

    async _onEvent(event: SpecEvent<StringKeyMap>, options?: StringKeyMap) {
        // Ensure we're actually subscribed to this event.
        const sub = this.eventSubs[event.name]
        if (!sub) {
            logger.error(`Got event for ${event.name} without subscription...something's wrong.`)
            return
        }

        // Prevent duplicates.
        if (this._wasEventSeen(event.id)) {
            logger.warn(`Duplicate event seen - ${event.id} - skipping.`)
            return
        }
        this._registerEventAsSeen(event)
        
        // Buffer new event if still resolving previous missed events.
        if (sub.shouldBuffer || options?.forceToBuffer) {
            this.eventSubs[event.name].buffer.push(event)
            return
        }

        this._processEvent(event)
        this._updateEventCursor(event)
    }

    async _processEvent(event: SpecEvent<StringKeyMap>) {
        // Get sub for event.
        const sub = this.eventSubs[event.name]
        if (!sub) {
            logger.error(`Processing event for ${event.name} without subscription...something's wrong.`)
            return
        }

        // Apply the event to each live object that depends on it.
        for (const liveObjectId of sub.liveObjectIds || []) {
            const liveObject = this.liveObjects[liveObjectId]
            if (!liveObject) continue

            const onError = (err: any) => logger.error(
                `Failed to apply event to live object - 
                (event=${event.name}; liveObject=${liveObjectId}): ${err}`
            )
            try {
                new ApplyEventService(event, liveObject).perform().catch(onError)
            } catch (err) {
                onError(err)
            }
        }
    }

    async _getLiveObjectsInConfig(): Promise<LiveObject[]> {
        // Get the list of live object version ids that haven't already been fetched.
        const newlyDetectedLiveObjectVersionIds = config.liveObjectIds.filter(
            id => !this.liveObjects.hasOwnProperty(id)
        )
        if (!newlyDetectedLiveObjectVersionIds.length) {
            logger.info('No new live objects detected.')
            return []
        }
        
        // Fetch the newly detected live objects via rpc.
        const newLiveObjects = await resolveLiveObjects(newlyDetectedLiveObjectVersionIds)
        if (newLiveObjects === null) {
            logger.error(`Failed to fetch new live objects: ${newlyDetectedLiveObjectVersionIds.join(', ')}.`)
            return []
        }

        // Add them to the live objects map.
        for (let liveObject of newLiveObjects) {
            this.liveObjects[liveObject.id] = liveObject
        }

        return newLiveObjects
    }

    _subscribeToLiveObjectEvents(): string[] {
        const liveObjectsByEvent = this._mapLiveObjectsByEvent()
        const newEventNames = []

        // Subscribe to new events.
        for (const newEventName in liveObjectsByEvent) {
            if (!this.eventSubs.hasOwnProperty(newEventName)) {
                // Register event callback.
                messageClient.on(
                    newEventName,
                    (event: SpecEvent<StringKeyMap>) => this._onEvent(event),
                )

                // Register sub.
                this.eventSubs[newEventName] = {
                    name: newEventName,
                    liveObjectIds: liveObjectsByEvent[newEventName],
                    cursor: null,
                    cursorChanged: false,
                    shouldBuffer: true,
                    buffer: [],
                }
                newEventNames.push(newEventName)
            }
        }

        // Unsubscribe from events that aren't needed anymore.
        this._removeUselessSubs(liveObjectsByEvent)

        return newEventNames
    }

    async _loadEventCursors() {
        // Get event subs that haven't been registered yet in the eventCursors map.
        const eventNamesWithNoCursor = Object.keys(this.eventSubs).filter(
            eventName => !this.eventSubs[eventName].cursor
        )

        // Get the missing event cursors from Postgres.
        const records = await getEventCursorsForNames(eventNamesWithNoCursor)
        for (let eventCursor of records) {
            this.eventSubs[eventCursor.name].cursor = eventCursor
        }
    }

    async _fetchMissedEvents(newEventNames: string[]) {
        // Get the previous cursors for the new events. 
        const cursors = []
        for (let newEventName of newEventNames) {
            const cursor = this.eventSubs[newEventName].cursor
            cursor && cursors.push(cursor)
        }
        if (!cursors.length) return

        logger.info('Fetching any missed events...')

        // Fetch any events that came after the following cursors.
        try {
            await messageClient.fetchMissedEvents(cursors, (events: SpecEvent<StringKeyMap>[]) => {
                events.forEach(event => this._onEvent(event, { forceToBuffer: true }))
            })
        } catch (error) {
            // TODO: Retry a few times...
        }

        logger.info('Events in-sync.')
    }

    async _upsertAndSeedLiveColumns() {
        // Upsert any new/changed live columns listed in the config.
        const upsertLiveColumnService = new UpsertLiveColumnsService()
        try {
            await upsertLiveColumnService.perform() 
        } catch (err) {
            logger.error(`Failed to upsert live columns: ${err}`)
            return
        }

        // We will seed (or re-seed) all live columns that were upserted.
        const liveColumnsToSeed = upsertLiveColumnService.liveColumnsToUpsert
        if (!liveColumnsToSeed.length) return

        const tablePathsUsingLiveObjectId = upsertLiveColumnService.tablePathsUsingLiveObjectId

        // Get a map of unique live-object/table relations (grouping the column names).
        const uniqueLiveObjectTablePaths = {}
        const tablePathsUsingLiveObjectIdForSeed: { [key: string]: Set<string> } = {}
        for (const { columnPath, liveProperty } of liveColumnsToSeed) {
            const [schemaName, tableName, colName] = columnPath.split('.')
            const [liveObjectId, _] = liveProperty.split(':')
            const tablePath = [schemaName, tableName].join('.')
            const uniqueKey = [liveObjectId, tablePath].join(':')
            if (!uniqueLiveObjectTablePaths.hasOwnProperty(uniqueKey)) {
                uniqueLiveObjectTablePaths[uniqueKey] = []
            }
            uniqueLiveObjectTablePaths[uniqueKey].push(colName)
            if (!tablePathsUsingLiveObjectIdForSeed.hasOwnProperty(liveObjectId)) {
                tablePathsUsingLiveObjectIdForSeed[liveObjectId] = new Set<string>()
            }
            if (!tablePathsUsingLiveObjectIdForSeed[liveObjectId].has(tablePath)) {
                tablePathsUsingLiveObjectIdForSeed[liveObjectId].add(tablePath)
            }
        }

        // Create unique live-object/table seed specs to perform.
        const seedSpecs: SeedSpec[] = []
        for (const uniqueKey in uniqueLiveObjectTablePaths) {
            const seedColNames = uniqueLiveObjectTablePaths[uniqueKey]
            const [liveObjectId, tablePath] = uniqueKey.split(':')
            const link = config.getLink(liveObjectId, tablePath)
            if (!link || !link.properties) {
                logger.error(
                    `No link properties found for liveObjectId: ${liveObjectId}, 
                    tablePath: ${tablePath}...something's wrong.`
                )
                seedFailed(seedColNames.map(colName => [tablePath, colName].join('.')))
                continue
            }
            seedSpecs.push({
                liveObjectId,
                tablePath,
                linkProperties: link.properties,
                seedColNames,
                seedIfEmpty: link.seedIfEmpty || false,
            })
        }

        seedSpecs.forEach(seedSpec => {
            const numTablesUsingLiveObject = tablePathsUsingLiveObjectId[seedSpec.liveObjectId].size
            const numTablesUsingLiveObjectForSeed = tablePathsUsingLiveObjectIdForSeed[seedSpec.liveObjectId].size
            // If this live object is only used in the table(s) about to be seeded, 
            // then add it to a list to indicates that events should be ignored.
            if (numTablesUsingLiveObject === numTablesUsingLiveObjectForSeed) {
                this.liveObjectsToIgnoreEventsFrom.add(seedSpec.liveObjectId)
            }
        })

        // Seed tables.
        seedSpecs.map(seedSpec => this._seedTable(seedSpec))
    }

    async _seedTable(seedSpec: SeedSpec) {
        const { liveObjectId, seedColNames, tablePath } = seedSpec

        try {
            const liveObject = this.liveObjects[liveObjectId]
            if (!liveObject) throw `No live object found for id ${liveObjectId}`
            await new SeedTableService(seedSpec, liveObject).perform()
        } catch (err) {
            logger.error(`Seed failed - ${err}`)
            seedFailed(seedColNames.map(colName => [tablePath, colName].join('.')))
            return
        }

        // Mark seed as success.
        seedSucceeded(seedColNames.map(colName => [tablePath, colName].join('.')))

        // Start listening to events from this live object now.
        this.liveObjectsToIgnoreEventsFrom.delete(liveObjectId)
        this._subscribeToLiveObjectEvents()
    }

    async _processAllBufferedEvents() {
        let promises = []
        for (let eventName in this.eventSubs) {
            promises.push(this._processEventBuffer(eventName))
        }
        await Promise.all(promises)
    }

    async _processEventBuffer(eventName: string) {
        // Sort buffered events oldest-to-newest.
        await this._sortEventBuffer(eventName)

        // Process each event (but don't await the processing).
        let event
        while (this.eventSubs[eventName].buffer.length > 0) {
            event = this.eventSubs[eventName].buffer.shift()
            this._processEvent(event)
        }

        // Turn buffer off and use the last seen event as the new cursor.
        this.eventSubs[eventName].shouldBuffer = false
        event && this._updateEventCursor(event)
    }

    async _sortEventBuffer(eventName: string): Promise<void> {
        return new Promise(async (res, _) => {
            while (true) {
                const buffer = this.eventSubs[eventName].buffer
                if (!buffer.length) break
    
                // Sort buffer by nonce (smallest none first).
                const sortedBuffer = [...buffer].sort((a, b) => a.nonce - b.nonce)
                
                // If some new event was buffered (race condition) during ^this sort,
                // try again.
                if (sortedBuffer.length !== this.eventSubs[eventName].buffer.length) {
                    continue
                }
    
                this.eventSubs[eventName].buffer = sortedBuffer
                break
            }
            res()
        })
    }

    async _saveEventCursors() {
        // Get all event cursors that changed since the last save interval.
        const cursorsToSave = []
        for (let eventName in this.eventSubs) {
            if (this.eventSubs[eventName].cursorChanged) {
                cursorsToSave.push(this.eventSubs[eventName].cursor)
                this.eventSubs[eventName].cursorChanged = false
            }
        }
        cursorsToSave.length && await saveEventCursors(cursorsToSave)
    }

    _doneProcessingNewConfig() {
        if (this.hasPendingConfigUpdate) {
            this._onNewConfig()
            return
        }

        this.isProcessingNewConfig = false
    }

    _createSaveCursorsJob() {
        if (this.saveEventCursorsJob === null) {
            this.saveEventCursorsJob = setInterval(
                () => this._saveEventCursors(),
                constants.SAVE_EVENT_CURSORS_INTERVAL,
            )
        }
    }

    _removeUselessSubs(liveObjectsByEvent: { [key: string]: string[] }) {
        for (const oldEventName in this.eventSubs) {
            if (!liveObjectsByEvent.hasOwnProperty(oldEventName)) {
                messageClient.off(oldEventName)
                const eventCursor = this.eventSubs[oldEventName].cursor
                saveEventCursors([eventCursor])
                delete this.eventSubs[oldEventName]
            }
        }
    }

    _mapLiveObjectsByEvent(): { [key: string]: string[] } {
        const subs = {}
        for (const liveObjectId in this.liveObjects) {
            // Ignore events from live objects actively/exclusively being used to seed.
            if (this.liveObjectsToIgnoreEventsFrom.has(liveObjectId)) continue

            const eventNames = this.liveObjects[liveObjectId].events.map(e => e.name)
            for (const eventName of eventNames) {
                if (!subs.hasOwnProperty(eventName)) {
                    subs[eventName] = []
                }
                subs[eventName].push(liveObjectId)
            }
        }
        return subs
    }

    _registerEventAsSeen(event: SpecEvent<StringKeyMap>) {
        this.seenEvents.set(event.id, true)
    }

    _wasEventSeen(eventId: string): boolean {
        return this.seenEvents.has(eventId)
    }

    _updateEventCursor(event: SpecEvent<StringKeyMap>) {
        this.eventSubs[event.name].cursor = {
            name: event.name,
            id: event.id,
            nonce: event.nonce,
            timestamp: event.origin.eventTimestamp,
        }
        this.eventSubs[event.name].cursorChanged = true
    }

    async _upsertTableSubs() {
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
        const subscribedChannels = tableSubscriber.getSubscribedChannels()
        if (subscribedChannels.includes(constants.TABLE_SUBS_CHANNEL)) return

        // Register event handler.
        tableSubscriber.notifications.on(
            constants.TABLE_SUBS_CHANNEL,
            event => this._onTableDataChange(event),
        )

        // Start listening to tables.
        try {
            await tableSubscriber.connect()
            await tableSubscriber.listenTo(constants.TABLE_SUBS_CHANNEL)
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

        // Parse primary keys in case of numerics.
        event.primaryKeys = this._parsePrimaryKeys(event.primaryKeys, tableSub.primaryKeyTypes)
        
        console.log(event)
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

        let currentPrimaryKeyCols, currentPrimaryKeys

        // If the insert trigger already exists, ensure the primary keys for the table haven't changed.
        if (insertTrigger) {
            try {
                currentPrimaryKeyCols = await getPrimaryKeys(tablePath, true)
            } catch (err) {
                logger.error(`Error fetching primary keys for ${tablePath}`)
            }
            if (currentPrimaryKeyCols) {
                currentPrimaryKeys = currentPrimaryKeyCols.map(pk => pk.name)
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
        }

        // If the update trigger already exists, ensure the primary keys for the table haven't changed.
        if (updateTrigger) {
            try {
                currentPrimaryKeyCols = currentPrimaryKeyCols || (await getPrimaryKeys(tablePath, true))
            } catch (err) {
                logger.error(`Error fetching primary keys for ${tablePath}`)
            }
            if (currentPrimaryKeyCols) {
                currentPrimaryKeys = currentPrimaryKeyCols.map(pk => pk.name)
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
        }

        try {
            currentPrimaryKeyCols = currentPrimaryKeyCols || (await getPrimaryKeys(tablePath, true))
            currentPrimaryKeys = currentPrimaryKeyCols.map(pk => pk.name)
        } catch (err) {
            logger.error(`Error fetching primary keys for ${tablePath}`)
            return
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
        const objects = config.liveObjects

        const registerTableSub = tablePath => {
            if (this.tableSubs.hasOwnProperty(tablePath)) {
                return
            }
            const [schema, table] = tablePath.split('.')
            this.tableSubs[tablePath] = {
                status: TableSubStatus.Pending,
                schema,
                table,
            }
        }

        for (const configName in objects) {
            const obj = objects[configName]
            const links = obj.links || []

            for (const link of links) {
                registerTableSub(link.table)

                for (const colPath of Object.values(link.properties || {})) {
                    const [colSchema, colTable, _] = colPath.split('.')
                    const colTablePath = [colSchema, colTable].join('.')
                    registerTableSub(colTablePath)
                }
            }
        }
    }
}

export default Spec