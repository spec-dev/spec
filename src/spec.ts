import logger from './lib/logger'
import config from './lib/config'
import constants from './lib/constants'
import { resolveLiveObjects } from './lib/rpcs/liveObjects'
import messageClient from './lib/rpcs/messageClient'
import { LiveObject, StringKeyMap, EventSub } from './lib/types'
import { ensureSpecSchemaIsReady, getEventCursorsForNames, saveEventCursors } from './lib/db/spec'
import { SpecEvent } from '@spec.dev/event-client'
import LRU from 'lru-cache'

class Spec {

    liveObjects: { [key: string]: LiveObject } = {}

    subs: { [key: string]: EventSub } = {}

    saveEventCursorsJob: any = null

    processingNewConfig: boolean = false

    pendingConfigUpdate: boolean = false
    
    seenEvents: LRU<string, boolean> = new LRU<string, boolean>({
        max: 5000,
    })
    
    async start() {
        // Run anytime message client socket connects.
        messageClient.onConnect = () => this._onMessageClientConnected()

        // Ensure the 'spec' schema and associated tables exist.
        await ensureSpecSchemaIsReady()

        // Subscribe to any config file changes.
        config.onUpdate = () => {
            if (this.processingNewConfig) {
                this.pendingConfigUpdate = true
            } else {
                this._onNewConfig()
            }
        }
        config.watch()

        // Force register an update to kick things off.
        config.onUpdate()
    }

    async _onNewConfig() {
        this.processingNewConfig = true

        // Load and validate the project config file.
        config.load()
        config.validate()

        // If the config is invalid, just wait until the next save to try again.
        if (!config.isValid) {
            this._doneProcessingNewConfig()
            return
        }

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

        // Subscribe to all events powering the live objects.
        const newEventNames = this._subscribeToLiveObjectEvents()
        if (!Object.keys(this.subs).length) {
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
        const sub = this.subs[event.name]
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
            this.subs[event.name].buffer.push(event)
            return
        }

        this._processEvent(event)
        this._updateEventCursor(event)
    }

    async _processEvent(event: SpecEvent<StringKeyMap>) {
        logger.info('Processing event...', event)
    }

    async _getLiveObjectsInConfig() {
        // Get the list of live object version ids that haven't already been fetched.
        const newlyDetectedLiveObjectVersionIds = config.liveObjectVersionIds.filter(
            id => !this.liveObjects.hasOwnProperty(id)
        )
        if (!newlyDetectedLiveObjectVersionIds.length) {
            logger.info('No new live objects detected.')
            return
        }
        
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
        const liveObjectsByEvent = this._mapLiveObjectByEvent()
        const newEventNames = []

        // Subscribe to new events.
        for (const newEventName in liveObjectsByEvent) {
            if (!this.subs.hasOwnProperty(newEventName)) {
                // Register event callback.
                messageClient.on(
                    newEventName,
                    (event: SpecEvent<StringKeyMap>) => this._onEvent(event),
                )

                // Register sub.
                this.subs[newEventName] = {
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
        const eventNamesWithNoCursor = Object.keys(this.subs).filter(
            eventName => !this.subs[eventName].cursor
        )

        // Get the missing event cursors from Postgres.
        const records = await getEventCursorsForNames(eventNamesWithNoCursor)
        for (let eventCursor of records) {
            this.subs[eventCursor.name].cursor = eventCursor
        }
    }

    async _fetchMissedEvents(newEventNames: string[]) {
        // Get the previous cursors for the new events. 
        const cursors = []
        for (let newEventName of newEventNames) {
            const cursor = this.subs[newEventName].cursor
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

    async _processAllBufferedEvents() {
        let promises = []
        for (let eventName in this.subs) {
            promises.push(this._processEventBuffer(eventName))
        }
        await Promise.all(promises)
    }

    async _processEventBuffer(eventName: string) {
        // Sort buffered events oldest-to-newest.
        await this._sortEventBuffer(eventName)

        // Process each event (but don't await the processing).
        let event
        while (this.subs[eventName].buffer.length > 0) {
            event = this.subs[eventName].buffer.shift()
            this._processEvent(event)
        }

        // Turn buffer off and use the last seen event as the new cursor.
        this.subs[eventName].shouldBuffer = false
        event && this._updateEventCursor(event)
    }

    async _sortEventBuffer(eventName: string): Promise<void> {
        return new Promise(async (res, _) => {
            while (true) {
                const buffer = this.subs[eventName].buffer
                if (!buffer.length) break
    
                // Sort buffer by nonce (smallest none first).
                const sortedBuffer = [...buffer].sort((a, b) => a.nonce - b.nonce)
                
                // If some new event was buffered (race condition) during ^this sort,
                // try again.
                if (sortedBuffer.length !== this.subs[eventName].buffer.length) {
                    continue
                }
    
                this.subs[eventName].buffer = sortedBuffer
                break
            }
            res()
        })
    }

    async _saveEventCursors() {
        // Get all event cursors that changed since the last save interval.
        const cursorsToSave = []
        for (let eventName in this.subs) {
            if (this.subs[eventName].cursorChanged) {
                cursorsToSave.push(this.subs[eventName].cursor)
                this.subs[eventName].cursorChanged = false
            }
        }
        cursorsToSave.length && await saveEventCursors(cursorsToSave)
    }

    _doneProcessingNewConfig() {
        if (this.pendingConfigUpdate) {
            this._onNewConfig()
            return
        }

        this.processingNewConfig = false
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
        for (const oldEventName in this.subs) {
            if (!liveObjectsByEvent.hasOwnProperty(oldEventName)) {
                messageClient.off(oldEventName)
                const eventCursor = this.subs[oldEventName].cursor
                saveEventCursors([eventCursor])
                delete this.subs[oldEventName]
            }
        }
    }

    _mapLiveObjectByEvent(): { [key: string]: string[] } {
        const subs = {}
        for (const liveObjectId in this.liveObjects) {
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
        this.subs[event.name].cursor = {
            name: event.name,
            id: event.id,
            nonce: event.nonce,
            timestamp: event.origin.eventTimestamp,
        }
        this.subs[event.name].cursorChanged = true
    }
}

export default Spec