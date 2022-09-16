import logger from './lib/logger'
import config from './lib/config'
import constants from './lib/constants'
import { resolveLiveObjects } from './lib/rpcs/liveObjects'
import messageClient from './lib/rpcs/messageClient'
import {
    LiveObject,
    StringKeyMap,
    EventSub,
    SeedSpec,
    SeedCursorStatus,
    SeedCursorJobType,
    ResolveRecordsSpec,
} from './lib/types'
import {
    ensureSpecSchemaIsReady,
    getEventCursorsForNames,
    saveEventCursors,
    getSeedCursorsWithStatus,
    seedFailed,
    seedSucceeded,
    processSeedCursorBatch,
    failedSeedCursorsExist,
} from './lib/db/spec'
import { SpecEvent } from '@spec.dev/event-client'
import LRU from 'lru-cache'
import ApplyEventService from './lib/services/ApplyEventService'
import UpsertLiveColumnsService from './lib/services/UpsertLiveColumnsService'
import SeedTableService from './lib/services/SeedTableService'
import { tableSubscriber } from './lib/db/subscriber'
import short from 'short-uuid'
import ResolveRecordsService from './lib/services/ResolveRecordsService'
import { unique } from './lib/utils/formatters'
import { getRecordsForPrimaryKeys } from './lib/db/ops'

class Spec {
    liveObjects: { [key: string]: LiveObject } = {}

    eventSubs: { [key: string]: EventSub } = {}

    saveEventCursorsJob: any = null

    retrySeedCursorsJob: any = null

    isProcessingNewConfig: boolean = false

    hasPendingConfigUpdate: boolean = false

    liveObjectsToIgnoreEventsFrom: Set<string> = new Set()

    hasCalledUpsertAndSeedLiveColumns: boolean = false

    seenEvents: LRU<string, boolean> = new LRU({
        max: constants.SEEN_EVENTS_CACHE_SIZE,
    })

    async start() {
        logger.info('Starting Spec...')

        // Run any time the message client socket connects.
        messageClient.onConnect = () => this._onMessageClientConnected()

        // Ensure the 'spec' schema and associated tables within it exist.
        await ensureSpecSchemaIsReady()

        logger.info('Schema is ready...')

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

        logger.info('Loading config...')

        // Load and validate project config file.
        if (config.load()) {
            await config.validate()
        }

        logger.info('Passed config validation..', config.config, config.isValid)
        return

        // If the config is invalid, just wait until the next save to try again.
        if (!config.isValid) {
            this._doneProcessingNewConfig()
            return
        }

        // Upsert table subscriptions (listen to data changes).
        tableSubscriber.getLiveObject = (id) => this.liveObjects[id]
        tableSubscriber.upsertTableSubs()

        // Connect to event/rpc message client.
        // Force run the onConnect handler if already connected.
        messageClient.client ? messageClient.onConnect() : messageClient.connect()
    }

    async _onMessageClientConnected() {
        // Resolve all live objects for the versions listed in the config file.
        await this._getLiveObjectsInConfig()
        if (this.liveObjects === null) {
            this._doneProcessingNewConfig()
            return
        }

        // Upsert live columns listed in the config and start seeding with new ones.
        await this._upsertAndSeedLiveColumns()

        // Subscribe to all events powering the live objects.
        const newEventNames = this._subscribeToLiveObjectEvents()
        if (!Object.keys(this.eventSubs).length) {
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
        this._createSaveCursorsJobIfNotExists()
        this._doneProcessingNewConfig()
    }

    async _onEvent(event: SpecEvent<StringKeyMap | StringKeyMap[]>, options?: StringKeyMap) {
        // Ensure we're actually subscribed to this event.
        const sub = this.eventSubs[event.name]
        if (!sub) {
            logger.error(`Got event for ${event.name} without subscription...something's wrong.`)
            return
        }

        // Ensure at least one live object will process this event.
        const liveObjectIdsThatWillProcessEvent = (sub.liveObjectIds || []).filter(
            (liveObjectId) => !this.liveObjectsToIgnoreEventsFrom.has(liveObjectId)
        )
        if (!liveObjectIdsThatWillProcessEvent.length) return

        // Prevent duplicates.
        if (
            this._wasEventSeenByAllDependentLiveObjects(event.id, liveObjectIdsThatWillProcessEvent)
        ) {
            logger.warn(`Duplicate event seen - ${event.id} - skipping.`)
            return
        }
        this._registerEventAsSeen(event, liveObjectIdsThatWillProcessEvent)

        // Buffer new event if still resolving previous missed events.
        if (sub.shouldBuffer || options?.forceToBuffer) {
            this.eventSubs[event.name].buffer.push(event)
            return
        }

        this._processEvent(event)

        this._updateEventCursor(event)
    }

    async _processEvent(event: SpecEvent<StringKeyMap | StringKeyMap[]>) {
        // Get sub for event.
        const sub = this.eventSubs[event.name]
        if (!sub) {
            logger.error(
                `Processing event for ${event.name} without subscription...something's wrong.`
            )
            return
        }

        // Apply the event to each live object that depends on it.
        for (const liveObjectId of sub.liveObjectIds || []) {
            if (this.liveObjectsToIgnoreEventsFrom.has(liveObjectId)) continue

            const liveObject = this.liveObjects[liveObjectId]
            if (!liveObject) continue

            const onError = (err: any) =>
                logger.error(
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
            (id) => !this.liveObjects.hasOwnProperty(id)
        )
        if (!newlyDetectedLiveObjectVersionIds.length) return

        // Fetch the newly detected live objects via rpc.
        const newLiveObjects = await resolveLiveObjects(newlyDetectedLiveObjectVersionIds)
        if (newLiveObjects === null) {
            logger.error(
                `Failed to fetch new live objects: ${newlyDetectedLiveObjectVersionIds.join(', ')}.`
            )
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
            if (this.eventSubs.hasOwnProperty(newEventName)) continue

            // Register event callback.
            messageClient.on(newEventName, (event: SpecEvent<StringKeyMap | StringKeyMap[]>) =>
                this._onEvent(event)
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

        // Unsubscribe from events that aren't needed anymore.
        this._removeUselessSubs(liveObjectsByEvent)

        return newEventNames
    }

    async _loadEventCursors(eventNamesFilter?: string[]) {
        eventNamesFilter = eventNamesFilter || []

        // Get event subs that haven't been registered yet in the eventCursors map.
        let eventNamesWithNoCursor = Object.keys(this.eventSubs).filter(
            (eventName) => !this.eventSubs[eventName].cursor
        )
        if (eventNamesFilter.length) {
            eventNamesWithNoCursor = eventNamesWithNoCursor.filter((eventName) =>
                eventNamesFilter.includes(eventName)
            )
        }

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

        return new Promise(async (res, _) => {
            // Fetch any events that came after the following cursors.
            try {
                await messageClient.fetchMissedEvents(
                    cursors,
                    (events: SpecEvent<StringKeyMap | StringKeyMap[]>[]) => {
                        logger.info(`Fetched ${events.length} missed events.`)
                        events.forEach((event) => this._onEvent(event, { forceToBuffer: true }))
                    },
                    () => {
                        logger.info('Events in-sync.')
                        res(null)
                    }
                )
            } catch (error) {
                logger.error(error)
                res(null)
                return
            }
        })
    }

    async _upsertAndSeedLiveColumns() {
        let liveColumnsToSeed = []

        // Upsert any new/changed live columns listed in the config.
        const upsertLiveColumnService = new UpsertLiveColumnsService()
        try {
            await upsertLiveColumnService.perform()
            liveColumnsToSeed = upsertLiveColumnService.liveColumnsToUpsert
        } catch (err) {
            logger.error(`Failed to upsert live columns: ${err}`)
            liveColumnsToSeed = []
        }

        // Seed (or re-seed) all live columns that were upserted.
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
            tablePathsUsingLiveObjectIdForSeed[liveObjectId].add(tablePath)
        }

        // Create unique live-object/table seed specs to perform.
        const seedSpecs: SeedSpec[] = []
        for (const uniqueKey in uniqueLiveObjectTablePaths) {
            const seedColNames = uniqueLiveObjectTablePaths[uniqueKey]
            const [liveObjectId, tablePath] = uniqueKey.split(':')
            const link = config.getLink(liveObjectId, tablePath)
            if (!link || !link.inputs) {
                logger.error(
                    `Not seeding table -- no link or link inputs found for (liveObjectId: ${liveObjectId} + 
                    tablePath: ${tablePath})...something's wrong.`
                )
                continue
            }
            seedSpecs.push({
                liveObjectId,
                tablePath,
                linkProperties: link.inputs,
                seedWith: link.seedWith,
                uniqueBy: link.uniqueBy || null,
                seedColNames,
                seedIfEmpty: link.seedIfEmpty || false,
            })
        }

        const seedSpecsMap = {}
        for (const seedSpec of seedSpecs) {
            seedSpecsMap[`${seedSpec.liveObjectId}:${seedSpec.tablePath}`] = seedSpec
        }

        // Get any seed jobs that failed (plus those that were stopped mid-seed (in-progress)
        // if this is our first pass through this function (i.e. on startup)).
        const statusesToRetry = [
            SeedCursorStatus.Failed,
            this.hasCalledUpsertAndSeedLiveColumns ? null : SeedCursorStatus.InProgress,
        ].filter((v) => !!v)
        const seedCursorsToRetry = (await getSeedCursorsWithStatus(statusesToRetry)) || []

        this.hasCalledUpsertAndSeedLiveColumns = true

        const retryResolveRecordsJobs = []
        const retrySeedTableJobs = []
        const deleteSeedCursorIds = []

        // Make sure there's not a conflict with seedSpecs above.
        // Merge seeds if that's the case.
        for (const seedCursor of seedCursorsToRetry) {
            const { liveObjectId, tablePath } = seedCursor.spec

            // Ensure this link still exists...
            if (!config.getLink(liveObjectId, tablePath)) {
                deleteSeedCursorIds.push(seedCursor.id)
                continue
            }

            if (seedCursor.jobType === SeedCursorJobType.SeedTable) {
                // Check to see if a seed spec for this liveObjectId+tablePath
                // is already scheduled to run (per above).
                const uniqueSeedSpecKey = `${liveObjectId}:${tablePath}`
                const plannedSeedSpec = seedSpecsMap[uniqueSeedSpecKey]

                // If a matching seed spec exists, just merge seedColNames and continue.
                if (plannedSeedSpec) {
                    const [schemaName, tableName] = tablePath.split('.')
                    const plannedSeedColNames = new Set(plannedSeedSpec.seedColNames || [])
                    const cursorSeedColNames = seedCursor.spec.seedColNames || []
                    const currentTableLiveColNames = new Set(
                        Object.keys(config.getTable(schemaName, tableName))
                    )
                    for (const colName of cursorSeedColNames) {
                        if (
                            !plannedSeedColNames.has(colName) &&
                            currentTableLiveColNames.has(colName)
                        ) {
                            seedSpecsMap[uniqueSeedSpecKey].seedColNames.push(colName)
                        }
                    }
                    deleteSeedCursorIds.push(seedCursor.id)
                } else {
                    retrySeedTableJobs.push(seedCursor)
                }
            } else if (seedCursor.jobType === SeedCursorJobType.ResolveRecords) {
                retryResolveRecordsJobs.push(seedCursor)
            }
        }

        const seedSpecsWithCursors = []

        // Compile instructions for new seed cursors to create.
        const createSeedCursors = []
        for (const seedSpec of seedSpecs) {
            const seedCursor = {
                id: short.generate(),
                jobType: SeedCursorJobType.SeedTable,
                spec: seedSpec,
                status: SeedCursorStatus.InProgress,
                cursor: 0,
            }
            createSeedCursors.push(seedCursor)
            seedSpecsWithCursors.push([seedSpec, seedCursor])
        }

        // Curate list of seed cursor ids to flip back to in-progress, and register
        // the seed cursors that need retrying with the master list of seed specs to run.
        const updateSeedCursorIds = []
        for (const seedCursor of retrySeedTableJobs) {
            const spec = seedCursor.spec as SeedSpec
            const { liveObjectId, tablePath } = spec
            if (!tablePathsUsingLiveObjectIdForSeed.hasOwnProperty(liveObjectId)) {
                tablePathsUsingLiveObjectIdForSeed[liveObjectId] = new Set<string>()
            }
            tablePathsUsingLiveObjectIdForSeed[liveObjectId].add(tablePath)
            updateSeedCursorIds.push(seedCursor.id)
            seedSpecsWithCursors.push([spec, seedCursor])
        }

        seedSpecsWithCursors.forEach(([seedSpec, _]) => {
            if (
                !tablePathsUsingLiveObjectId.hasOwnProperty(seedSpec.liveObjectId) ||
                !tablePathsUsingLiveObjectIdForSeed.hasOwnProperty(seedSpec.liveObjectId)
            ) {
                return
            }

            const numTablesUsingLiveObject = tablePathsUsingLiveObjectId[seedSpec.liveObjectId].size
            const numTablesUsingLiveObjectForSeed =
                tablePathsUsingLiveObjectIdForSeed[seedSpec.liveObjectId].size

            // If this live object is only used in the table(s) about to be seeded,
            // then add it to a list to indicates that events should be ignored.
            if (numTablesUsingLiveObject === numTablesUsingLiveObjectForSeed) {
                this.liveObjectsToIgnoreEventsFrom.add(seedSpec.liveObjectId)
            }
        })

        // Create seed jobs and determine the seed strategies up-front for each.
        const seedJobs = {}
        for (const [seedSpec, seedCursor] of seedSpecsWithCursors) {
            const { liveObjectId, tablePath } = seedSpec
            const liveObject = this.liveObjects[liveObjectId]
            if (!liveObject) continue

            try {
                const seedTableService = new SeedTableService(
                    seedSpec,
                    liveObject,
                    seedCursor.id,
                    seedCursor.cursor
                )

                // Determine seed strategy up-front unless already determined
                // (see '_processExternalTableLinkDataChanges()' within subscriber.ts)
                if (!seedCursor.metadata?.foreignTablePath) {
                    await seedTableService.determineSeedStrategy()
                }
                seedJobs[tablePath] = seedJobs[tablePath] || {
                    seedTableJobs: [],
                    resolveRecordsJobs: [],
                }
                seedJobs[tablePath].seedTableJobs.push([
                    seedTableService,
                    seedSpec,
                    seedCursor.metadata,
                ])
            } catch (err) {
                logger.error(`Creating seed table service for ${tablePath} failed: ${err}`)
                continue
            }
        }

        // Create resolve records jobs (if need be).
        for (const seedCursor of retryResolveRecordsJobs) {
            const resolveRecordsSpec = seedCursor.spec as ResolveRecordsSpec
            const { liveObjectId, tablePath } = resolveRecordsSpec
            const liveObject = this.liveObjects[liveObjectId]

            if (!liveObject) {
                deleteSeedCursorIds.push(seedCursor.id)
                continue
            }

            const link = config.getLink(liveObjectId, tablePath)
            if (!link) {
                deleteSeedCursorIds.push(seedCursor.id)
                continue
            }

            try {
                const resolveRecordsService = new ResolveRecordsService(
                    resolveRecordsSpec,
                    liveObject,
                    link,
                    seedCursor.id,
                    seedCursor.cursor
                )
                seedJobs[tablePath] = seedJobs[tablePath] || {
                    seedTableJobs: [],
                    resolveRecordsJobs: [],
                }
                seedJobs[tablePath].resolveRecordsJobs.push([
                    resolveRecordsService,
                    resolveRecordsSpec,
                ])
            } catch (err) {
                logger.error(`Creating resolve records service for ${tablePath} failed: ${err}`)
                continue
            }

            updateSeedCursorIds.push(seedCursor.id)
        }

        // Save seed cursor inserts/updates/deletes as a single batch transaction.
        const saved = await processSeedCursorBatch(
            createSeedCursors,
            updateSeedCursorIds,
            deleteSeedCursorIds
        )
        if (!saved) return

        // Run all seed jobs.
        Object.values(seedJobs).forEach((jobs) => {
            this._runSeedJobs(jobs).catch((err) => logger.error(err))
        })

        // Create a job that checks and retries failed seeds on an interval.
        this._createRetrySeedCursorsJobIfNotExists()
    }

    async _runSeedJobs(jobs: StringKeyMap) {
        const seedTableJobs = jobs.seedTableJobs || []
        const resolveRecordsJobs = jobs.resolveRecordsJobs || []
        const seedLiveObjectIds = unique(seedTableJobs.map((j) => j[1].liveObjectId))
        const resolveLiveObjectIds = unique(resolveRecordsJobs.map((j) => j[1].liveObjectId))
        const liveObjectIdsOnlyInResolveJobs = resolveLiveObjectIds.filter(
            (liveObjectId) => !seedLiveObjectIds.includes(liveObjectId)
        )

        // Run all resolve records jobs first.
        await Promise.all(
            resolveRecordsJobs.map(([service, spec]) => this._resolveRecords(service, spec))
        )

        this._processEventsPostSeed(liveObjectIdsOnlyInResolveJobs)

        // Run all seed jobs.
        await Promise.all(
            seedTableJobs.map(([service, spec, metadata]) =>
                this._seedTable(service, spec, metadata || {})
            )
        )
    }

    async _resolveRecords(
        resolveRecordsService: ResolveRecordsService,
        resolveRecordsSpec: ResolveRecordsSpec
    ) {
        const { liveObjectId, tablePath } = resolveRecordsSpec
        const seedCursorId = resolveRecordsService.seedCursorId
        try {
            await resolveRecordsService.perform()
        } catch (err) {
            logger.error(
                `Failed to resolve records for (liveObjectId=${liveObjectId}, tablePath=${tablePath}): ${err}`
            )
            await seedFailed(seedCursorId)
        }

        // Register seed cursor as successful.
        await seedSucceeded(seedCursorId)
    }

    async _seedTable(
        seedTableService: SeedTableService,
        seedSpec: SeedSpec,
        metadata: StringKeyMap
    ) {
        const { liveObjectId, tablePath } = seedSpec
        const foreignTablePath = metadata?.foreignTablePath

        if (!!foreignTablePath) {
            const foreignPrimaryKeyData = metadata.foreignPrimaryKeyData || []
            try {
                const foreignRecords = await getRecordsForPrimaryKeys(
                    foreignTablePath,
                    foreignPrimaryKeyData
                )
                await seedTableService.seedWithForeignRecords(foreignTablePath, foreignRecords)
            } catch (err) {
                logger.error(`Seed failed for table ${tablePath}: ${err}`)
                seedFailed(seedTableService.seedCursorId)
                return
            }
        } else {
            try {
                await seedTableService.executeSeedStrategy()
            } catch (err) {
                logger.error(`Seed failed for table ${tablePath}: ${err}`)
                seedFailed(seedTableService.seedCursorId)
                return
            }
        }

        await Promise.all([
            seedSucceeded(seedTableService.seedCursorId),
            this._processEventsPostSeed([liveObjectId]),
        ])
    }

    async _processEventsPostSeed(liveObjectIds: string[]) {
        // Start listening to events from these live objects now.
        liveObjectIds.forEach((liveObjectId) => {
            this.liveObjectsToIgnoreEventsFrom.delete(liveObjectId)
        })

        const newEventNames = this._subscribeToLiveObjectEvents() || []
        if (!newEventNames.length) return

        // Load the event cursors for these new events.
        await this._loadEventCursors(newEventNames)

        // Fetch missed events for any new sub that already has an
        // existing event cursor (i.e. events that have been seen before).
        await this._fetchMissedEvents(newEventNames)

        // Process all missed events just added to the buffer above.
        await this._processAllBufferedEvents(newEventNames)

        // Start saving event cursors on an interval (+ save immediately).
        this._saveEventCursors(newEventNames)
        this._createSaveCursorsJobIfNotExists()
    }

    async _processAllBufferedEvents(eventNamesFilter?: string[]) {
        eventNamesFilter = eventNamesFilter || []
        let promises = []
        for (let eventName in this.eventSubs) {
            if (eventNamesFilter.length && !eventNamesFilter.includes(eventName)) continue
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

    async _saveEventCursors(eventNamesFilter?: string[]) {
        eventNamesFilter = eventNamesFilter || []

        // Get all event cursors that changed since the last save interval.
        const cursorsToSave = []
        for (let eventName in this.eventSubs) {
            if (eventNamesFilter.length && !eventNamesFilter.includes(eventName)) continue

            if (this.eventSubs[eventName].cursorChanged) {
                cursorsToSave.push(this.eventSubs[eventName].cursor)
                this.eventSubs[eventName].cursorChanged = false
            }
        }

        cursorsToSave.length && (await saveEventCursors(cursorsToSave))
    }

    _doneProcessingNewConfig() {
        if (this.hasPendingConfigUpdate) {
            this.hasPendingConfigUpdate = false
            this._onNewConfig()
            return
        }
        this.isProcessingNewConfig = false
    }

    _createSaveCursorsJobIfNotExists() {
        this.saveEventCursorsJob =
            this.saveEventCursorsJob ||
            setInterval(() => this._saveEventCursors(), constants.SAVE_EVENT_CURSORS_INTERVAL)
    }

    _createRetrySeedCursorsJobIfNotExists() {
        this.retrySeedCursorsJob =
            this.retrySeedCursorsJob ||
            setInterval(() => this._retrySeedCursors(), constants.RETRY_SEED_CURSORS_INTERVAL)
    }

    async _retrySeedCursors() {
        if (!(await failedSeedCursorsExist())) return
        logger.info('Failed seed cursors exist....Will retry seed job(s).')
        await this._upsertAndSeedLiveColumns()
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

            const eventNames = this.liveObjects[liveObjectId].events.map((e) => e.name)
            for (const eventName of eventNames) {
                if (!subs.hasOwnProperty(eventName)) {
                    subs[eventName] = []
                }
                subs[eventName].push(liveObjectId)
            }
        }
        return subs
    }

    _registerEventAsSeen(event: SpecEvent<StringKeyMap | StringKeyMap[]>, liveObjectIds: string[]) {
        liveObjectIds.forEach((liveObjectId) => {
            this.seenEvents.set(`${event.id}:${liveObjectId}`, true)
        })
    }

    _wasEventSeenByAllDependentLiveObjects(eventId: string, liveObjectIds: string[]): boolean {
        for (let liveObjectId of liveObjectIds) {
            if (!this.seenEvents.has(`${eventId}:${liveObjectId}`)) {
                return false
            }
        }
        return true
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
