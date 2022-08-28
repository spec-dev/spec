import logger from './lib/logger'
import config from './lib/config'
import constants from './lib/constants'
import { resolveLiveObjects } from './lib/rpcs/liveObjects'
import messageClient from './lib/rpcs/messageClient'
import { LiveObject, StringKeyMap, EventSub, SeedSpec } from './lib/types'
import { ensureSpecSchemaIsReady, getEventCursorsForNames, saveEventCursors, seedFailed, seedSucceeded } from './lib/db/spec'
import { SpecEvent } from '@spec.dev/event-client'
import LRU from 'lru-cache'
import ApplyEventService from './lib/services/ApplyEventService'
import UpsertLiveColumnsService from './lib/services/UpsertLiveColumnsService'
import SeedTableService from './lib/services/SeedTableService'
import { tableSubscriber } from './lib/db/subscriber'

class Spec {

    liveObjects: { [key: string]: LiveObject } = {}

    eventSubs: { [key: string]: EventSub } = {}

    saveEventCursorsJob: any = null

    isProcessingNewConfig: boolean = false

    hasPendingConfigUpdate: boolean = false

    liveObjectsToIgnoreEventsFrom: Set<string> = new Set()
    
    seenEvents: LRU<string, boolean> = new LRU({
        max: constants.SEEN_EVENTS_CACHE_SIZE,
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

        // Load and validate project config file.
        if (config.load()) {
            await config.validate()
        }

        // If the config is invalid, just wait until the next save to try again.
        if (!config.isValid) {
            this._doneProcessingNewConfig()
            return
        }

        // Upsert table subscriptions (listen to data changes).
        tableSubscriber.getLiveObject = id => this.liveObjects[id]
        tableSubscriber.upsertTableSubs()

        // Connect to event/rpc message client. 
        // Force run the onConnect handler if already connected.
        messageClient.client ? messageClient.onConnect() : messageClient.connect()
    }

    async _onMessageClientConnected() {
        // Resolve all live objects for the versions listed in the config file.
        await this._getLiveObjectsInConfig()
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

    async _onEvent(event: SpecEvent<StringKeyMap | StringKeyMap[]>, options?: StringKeyMap) {
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

        // TODO: Consider not running this until _processEvent executes without failure.
        this._updateEventCursor(event)
    }

    async _processEvent(event: SpecEvent<StringKeyMap | StringKeyMap[]>) {
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

    async _getLiveObjectsInConfig() {
        // Get the list of live object version ids that haven't already been fetched.
        const newlyDetectedLiveObjectVersionIds = config.liveObjectIds.filter(
            id => !this.liveObjects.hasOwnProperty(id)
        )
        if (!newlyDetectedLiveObjectVersionIds.length) return
        
        // Fetch the newly detected live objects via rpc.
        const newLiveObjects = await resolveLiveObjects(newlyDetectedLiveObjectVersionIds)
        if (newLiveObjects === null) {
            logger.error(`Failed to fetch new live objects: ${newlyDetectedLiveObjectVersionIds.join(', ')}.`)
            return
        }

        // Add them to the live objects map.
        for (let liveObject of newLiveObjects) {
            this.liveObjects[liveObject.id] = liveObject
        }
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
                    (event: SpecEvent<StringKeyMap | StringKeyMap[]>) => this._onEvent(event),
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
            await messageClient.fetchMissedEvents(cursors, (events: SpecEvent<StringKeyMap | StringKeyMap[]>[]) => {
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

        // Seed (or re-seed) all live columns that were upserted.
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
                seedWith: link.seedWith,
                uniqueBy: link.uniqueBy,
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

        // Create seed table services and determine the seed strategies up-front for each.
        const seedTableJobs = []
        for (const seedSpec of seedSpecs) {
            const { liveObjectId, seedColNames, tablePath } = seedSpec
            try {
                const liveObject = this.liveObjects[liveObjectId]
                if (!liveObject) throw `No live object found for id ${liveObjectId}`
                const seedTableService = new SeedTableService(seedSpec, liveObject)
                await seedTableService.determineSeedStrategy()
                seedTableJobs.push([seedTableService, seedSpec])
            } catch (err) {
                logger.error(`Creating seed table service for ${tablePath} failed: ${err}`)
                seedFailed(seedColNames.map(colName => [tablePath, colName].join('.')))
            }
        }

        // Seed tables in parallel.
        seedTableJobs.forEach(seedTableJob => {
            const [seedTableService, seedSpec] = seedTableJob
            this._seedTable(seedTableService, seedSpec)
        })
    }

    async _seedTable(seedTableService: SeedTableService, seedSpec: SeedSpec) {
        const { liveObjectId, seedColNames, tablePath } = seedSpec

        try {
            await seedTableService.executeSeedStrategy()
        } catch (err) {
            logger.error(`Seed failed for table ${tablePath}: ${err}`)
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
        if (this.saveEventCursorsJob !== null) return
        
        this.saveEventCursorsJob = setInterval(
            () => this._saveEventCursors(),
            constants.SAVE_EVENT_CURSORS_INTERVAL,
        )
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

    _registerEventAsSeen(event: SpecEvent<StringKeyMap | StringKeyMap[]>) {
        this.seenEvents.set(event.id, true)
    }

    _wasEventSeen(eventId: string): boolean {
        return this.seenEvents.has(eventId)
    }

    _updateEventCursor(event: SpecEvent<StringKeyMap | StringKeyMap[]>) {
        this.eventSubs[event.name].cursor = {
            name: event.name,
            id: event.id,
            nonce: event.nonce,
            timestamp: event.origin.eventTimestamp,
        }
        this.eventSubs[event.name].cursorChanged = true
    }
}

export default Spec