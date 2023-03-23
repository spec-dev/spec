import logger from './lib/logger'
import config from './lib/config'
import { constants } from './lib/constants'
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
    updateStatus,
    getSeedCursorWaitingInLine,
} from './lib/db/spec'
import { SpecEvent } from '@spec.dev/event-client'
import LRU from 'lru-cache'
import ApplyEventService from './lib/services/ApplyEventService'
import UpsertLiveColumnsService from './lib/services/UpsertLiveColumnsService'
import SeedTableService from './lib/services/SeedTableService'
import { tableSubscriber } from './lib/db/subscriber'
import short from 'short-uuid'
import ResolveRecordsService from './lib/services/ResolveRecordsService'
import { getRecordsForPrimaryKeys } from './lib/db/ops'
import { importHooks } from './lib/hooks'
import { sleep } from './lib/utils/time'
import { unique } from './lib/utils/formatters'
import chalk from 'chalk'
import { db } from './lib/db'
import { importHandlers, getHandlers, CUSTOM_EVENT_HANDLER_KEY } from './lib/handlers'

class Spec {
    liveObjects: { [key: string]: LiveObject } = {}

    eventSubs: { [key: string]: EventSub } = {}

    customEventHandlers: { [key: string]: any } = {}

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
        await Promise.all([importHooks(), importHandlers()])
        this.customEventHandlers = getHandlers()

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
        tableSubscriber.getLiveObject = (id) => this.liveObjects[id]
        tableSubscriber.upsertTableSubs()

        // Connect to event/rpc message client.
        // Force run the onConnect handler if already connected.
        messageClient.client ? messageClient.onConnect() : messageClient.connect()
    }

    async _onMessageClientConnected() {
        // Resolve all live objects for the versions listed in the config file.
        await this._getLiveObjectsInConfig()

        // Upsert live columns listed in the config and start seeding with new ones.
        if (Object.keys(this.liveObjects).length) {
            await this._upsertAndSeedLiveColumns()
        }

        // Subscribe to all events powering the live objects,
        // as well as those used in custom event handlers.
        const newEventNames = this._subscribeToEvents()
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

    async _onEvent(event: SpecEvent, options?: StringKeyMap) {
        // Ensure we're actually subscribed to this event.
        const sub = this.eventSubs[event.name]
        if (!sub) {
            logger.error(`Got event for ${event.name} without subscription...something's wrong.`)
            return
        }

        const hasCustomEventHandler = this.customEventHandlers.hasOwnProperty(event.name)

        // Ensure at least one live object (or custom handler) will process this event.
        const liveObjectIdsThatWillProcessEvent = (sub.liveObjectIds || []).filter(
            (liveObjectId) => !this.liveObjectsToIgnoreEventsFrom.has(liveObjectId)
        )
        if (!liveObjectIdsThatWillProcessEvent.length && !hasCustomEventHandler) return

        // Prevent the re-processing of duplicates.
        const subjects = [...liveObjectIdsThatWillProcessEvent]
        if (hasCustomEventHandler) {
            subjects.push(CUSTOM_EVENT_HANDLER_KEY)
        }
        if (this._wasEventSeenByAll(event.id, subjects)) {
            logger.warn(`Duplicate event seen - ${event.id} - skipping.`)
            return
        }
        this._registerEventAsSeen(event, subjects)

        // Buffer new event if still resolving previous missed events.
        if (sub.shouldBuffer || options?.forceToBuffer) {
            this.eventSubs[event.name].buffer.push(event)
            return
        }

        this._processEvent(event)
        this._updateEventCursor(event)
    }

    async _processEvent(event: SpecEvent) {
        // Get sub for event.
        const sub = this.eventSubs[event.name]
        if (!sub) {
            logger.error(
                `Processing event for ${event.name} without subscription...something's wrong.`
            )
            return
        }

        // Apply the event to each live object that depends on it.
        let processedEvent = false
        for (const liveObjectId of sub.liveObjectIds || []) {
            if (this.liveObjectsToIgnoreEventsFrom.has(liveObjectId)) continue

            const liveObject = this.liveObjects[liveObjectId]
            if (!liveObject) continue

            processedEvent = true
            try {
                const service = new ApplyEventService(event, liveObject)
                await service.perform()
            } catch (err) {
                logger.error(
                    `Failed to apply event to live object - (event=${event.name}; ` +
                        `liveObject=${liveObjectId}): ${err?.message || err}`
                )
            }
        }

        // Run custom event handler (if exists).
        const customHandler = this.customEventHandlers[event.name]
        if (customHandler) {
            if (!processedEvent) {
                const origin = event.origin
                const chainId = origin?.chainId
                const blockNumber = origin?.blockNumber
                logger.info(
                    `[${chainId}:${blockNumber}] Processing ${event.name} (${event.nonce})...`
                )
            }
            try {
                await customHandler(event, db, logger)
            } catch (err) {
                logger.error(`Custom handler for ${event.name} failed: ${err?.message || err}`)
            }
        }
    }

    async _getLiveObjectsInConfig() {
        const liveObjects = await resolveLiveObjects(config.liveObjectIds, this.liveObjects)
        for (const liveObject of liveObjects) {
            this.liveObjects[liveObject.id] = liveObject
        }
    }

    _subscribeToEvents(): string[] {
        const liveObjectsByEvent = this._mapLiveObjectsByEvent()
        const newEventNames = []

        // Subscribe to live object events.
        for (const newEventName in liveObjectsByEvent) {
            // Update live object ids if something changed.
            if (this.eventSubs.hasOwnProperty(newEventName)) {
                const existingLiveObjectIds = (this.eventSubs[newEventName].liveObjectIds || [])
                    .sort()
                    .join(':')
                const newLiveObjectIds = (liveObjectsByEvent[newEventName] || []).sort().join(':')
                if (newLiveObjectIds !== existingLiveObjectIds) {
                    this.eventSubs[newEventName].liveObjectIds = liveObjectsByEvent[newEventName]
                }
                continue
            }

            // Subscribe to event.
            messageClient.on(newEventName, (event: SpecEvent) => this._onEvent(event))

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

        // Subscribe to events for custom handlers.
        for (const eventName in this.customEventHandlers) {
            if (this.eventSubs.hasOwnProperty(eventName)) continue
            messageClient.on(eventName, (event: SpecEvent) => this._onEvent(event))

            // Register sub with no live object ids.
            this.eventSubs[eventName] = {
                name: eventName,
                liveObjectIds: [],
                cursor: null,
                cursorChanged: false,
                shouldBuffer: true,
                buffer: [],
            }

            newEventNames.push(eventName)
        }

        return unique(newEventNames)
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
                    (events: SpecEvent[]) => {
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
        // We will seed (or re-seed) all live columns that were upserted.
        const upsertLiveColumnService = new UpsertLiveColumnsService()
        try {
            await upsertLiveColumnService.perform()
            liveColumnsToSeed = upsertLiveColumnService.liveColumnsToUpsert
        } catch (err) {
            logger.error(`Failed to upsert live columns: ${err}`)
            liveColumnsToSeed = []
        }

        const tablePathsUsingLiveObjectId = upsertLiveColumnService.tablePathsUsingLiveObjectId
        const newLiveTablePaths = upsertLiveColumnService.newLiveTablePaths

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
            const link = config.getEnrichedLink(liveObjectId, tablePath)
            if (!link || !link.linkOn) {
                logger.error(
                    `Not seeding table -- no link or link.linkOn found for (liveObjectId: ${liveObjectId} + 
                    tablePath: ${tablePath})...something's wrong.`
                )
                continue
            }
            seedSpecs.push({
                liveObjectId,
                tablePath,
                seedColNames,
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
            if (!config.getEnrichedLink(liveObjectId, tablePath)) {
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

        const seedSpecsToRunNow = []
        const seedSpecsOnNewLiveTables = []
        for (const seedSpec of seedSpecs) {
            if (newLiveTablePaths.has(seedSpec.tablePath)) {
                seedSpecsOnNewLiveTables.push(seedSpec)
            } else {
                seedSpecsToRunNow.push(seedSpec)
            }
        }
        seedSpecsOnNewLiveTables.sort(
            (a, b) => config.getTableOrder(a.tablePath) - config.getTableOrder(b.tablePath)
        )

        if (seedSpecsOnNewLiveTables.length) {
            logger.info(
                chalk.cyanBright(
                    `New live tables detected - will seed in series: ${seedSpecsOnNewLiveTables
                        .map((seedSpec) => seedSpec.tablePath)
                        .join(', ')}`
                )
            )
        }

        let seedSpecsToWaitInLine = []
        if (seedSpecsOnNewLiveTables.length >= 1) {
            seedSpecsToRunNow.push(seedSpecsOnNewLiveTables[0])
            seedSpecsToWaitInLine = seedSpecsOnNewLiveTables.slice(1)
        }

        const seedCursorsToWaitInLine: StringKeyMap[] = seedSpecsToWaitInLine.map((seedSpec) => ({
            id: short.generate(),
            jobType: SeedCursorJobType.SeedTable,
            spec: seedSpec,
            status: SeedCursorStatus.InLine,
            cursor: 0,
        }))
        for (let i = 0; i < seedCursorsToWaitInLine.length; i++) {
            if (i < seedCursorsToWaitInLine.length - 1) {
                seedCursorsToWaitInLine[i].metadata = {
                    nextId: seedCursorsToWaitInLine[i + 1].id,
                }
            }
        }

        // Compile instructions for new seed cursors to create.
        const createSeedCursors = [...seedCursorsToWaitInLine]
        const seedSpecsWithCursors = []
        for (let i = 0; i < seedSpecsToRunNow.length; i++) {
            const seedSpec = seedSpecsToRunNow[i]
            const seedCursor: StringKeyMap = {
                id: short.generate(),
                jobType: SeedCursorJobType.SeedTable,
                spec: seedSpec,
                status: SeedCursorStatus.InProgress,
                cursor: 0,
            }

            if (i === seedSpecsToRunNow.length - 1 && seedCursorsToWaitInLine.length) {
                seedCursor.metadata = {
                    nextId: seedCursorsToWaitInLine[0].id,
                }
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

        // Ignore events from live objects exclusively tied to seeds to wait on.
        seedSpecsToWaitInLine.forEach((seedSpec) => {
            const { liveObjectId } = seedSpec
            if (
                !tablePathsUsingLiveObjectId.hasOwnProperty(liveObjectId) ||
                !tablePathsUsingLiveObjectIdForSeed.hasOwnProperty(liveObjectId)
            ) {
                return
            }

            const numTablesUsingLiveObject = tablePathsUsingLiveObjectId[liveObjectId].size
            const numTablesUsingLiveObjectForSeed =
                tablePathsUsingLiveObjectIdForSeed[liveObjectId].size
            const waitOnLiveObjectEvents =
                numTablesUsingLiveObject === numTablesUsingLiveObjectForSeed
            waitOnLiveObjectEvents && this.liveObjectsToIgnoreEventsFrom.add(liveObjectId)
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
                    seedCursor.cursor,
                    seedCursor.metadata,
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

            const link = config.getEnrichedLink(liveObjectId, tablePath)
            if (!link) {
                deleteSeedCursorIds.push(seedCursor.id)
                continue
            }

            try {
                const resolveRecordsService = new ResolveRecordsService(
                    resolveRecordsSpec,
                    liveObject,
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
        // Run all resolve records jobs first.
        await Promise.all(
            (jobs.resolveRecordsJobs || []).map(([service, spec]) =>
                this._resolveRecords(service, spec)
            )
        )

        const normalSeedTableJobs = []
        const seedTableJobsInSeries = []
        for (const seedTableJob of jobs.seedTableJobs || []) {
            const [service, spec, metadata] = seedTableJob
            if (metadata?.nextId) {
                seedTableJobsInSeries.push(seedTableJob)
            } else {
                normalSeedTableJobs.push(seedTableJob)
            }
        }

        // Run all normal seed jobs.
        await Promise.all(
            normalSeedTableJobs.map(([service, spec, metadata]) =>
                this._seedTable(service, spec, metadata || {})
            )
        )

        // Run seed table jobs in series.
        seedTableJobsInSeries.forEach(([service, spec, metadata]) => {
            this._seedTable(service, spec, metadata || {})
        })
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
        const { tablePath } = seedSpec
        const foreignTablePath = metadata?.foreignTablePath
        const attempt = metadata?.attempts || 1
        const maxAttempts = constants.MAX_SEED_JOB_ATTEMPTS
        const failLogPrefix = `[${attempt}/${maxAttempts}] Seed failed for table ${tablePath}`

        if (!!foreignTablePath) {
            const foreignPrimaryKeyData = metadata.foreignPrimaryKeyData || []
            try {
                const foreignRecords = await getRecordsForPrimaryKeys(
                    foreignTablePath,
                    foreignPrimaryKeyData
                )
                await seedTableService.seedWithForeignRecords(foreignTablePath, foreignRecords)
            } catch (err) {
                logger.error(chalk.yellow(`${failLogPrefix}: ${err}`))
                seedFailed(seedTableService.seedCursorId)
                return
            }
        } else {
            try {
                await seedTableService.executeSeedStrategy()
            } catch (err) {
                logger.error(chalk.yellow(`${failLogPrefix}: ${err}`))
                seedFailed(seedTableService.seedCursorId)
                return
            }
        }

        await seedSucceeded(seedTableService.seedCursorId)

        // Run next seed cursor in series if one is registered.
        metadata?.nextId && this._runNextSeedCursorInSeries(metadata.nextId)
    }

    async _runNextSeedCursorInSeries(id: string) {
        // Find the 'in-line' seed cursor by id.
        const seedCursor = await getSeedCursorWaitingInLine(id)
        if (!seedCursor) {
            logger.error(`Next seed cursor in series (id=${id}) was missing...`)
            return
        }

        // Make sure the live object is still being used.
        const seedSpec = seedCursor.spec as SeedSpec
        const { liveObjectId } = seedSpec

        const liveObject = this.liveObjects[liveObjectId]
        if (!liveObject) {
            logger.error(
                `Live object ${liveObjectId} isn't registered anymore - skipping seed cursor in series.`
            )
            // Register success anyway and go to next in series.
            await seedSucceeded(seedCursor.id)
            seedCursor.metadata?.nextId &&
                this._runNextSeedCursorInSeries(seedCursor.metadata?.nextId)
            return
        }

        let seedTableService: SeedTableService
        try {
            seedTableService = new SeedTableService(
                seedSpec,
                liveObject,
                seedCursor.id,
                seedCursor.cursor,
                seedCursor.metadata,
            )

            // Determine seed strategy up-front unless already determined
            // (see '_processExternalTableLinkDataChanges()' within subscriber.ts)
            if (!seedCursor.metadata?.foreignTablePath) {
                await seedTableService.determineSeedStrategy()
            }
        } catch (err) {
            logger.error(
                `Creating seed table service for seed_cursor in series (id=${seedCursor.id}) failed: ${err}`
            )
            return
        }

        // Update seed cursor to in-progress and run it.
        await updateStatus(seedCursor.id, SeedCursorStatus.InProgress)
        this._seedTable(seedTableService, seedSpec, seedCursor.metadata)

        // Start to process events for this live object at the same time as starting the table seed.
        if (this.liveObjectsToIgnoreEventsFrom.has(liveObjectId)) {
            this._processEventsPostSeed([liveObjectId])
        }
    }

    async _processEventsPostSeed(liveObjectIds: string[]) {
        // Start listening to events from these live objects now.
        liveObjectIds.forEach((liveObjectId) => {
            this.liveObjectsToIgnoreEventsFrom.delete(liveObjectId)
        })

        const newEventNames = this._subscribeToEvents() || []
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

        // Process each event.
        let event
        while (this.eventSubs[eventName].buffer.length > 0) {
            event = this.eventSubs[eventName].buffer.shift()
            await this._processEvent(event)
            await sleep(5)
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
                const sortedBuffer = [...buffer].sort((a, b) => {
                    return (
                        parseFloat(a.nonce.replace('-', '.')) -
                        parseFloat(b.nonce.replace('-', '.'))
                    )
                })

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
            const unused =
                !liveObjectsByEvent.hasOwnProperty(oldEventName) &&
                !this.customEventHandlers.hasOwnProperty(oldEventName)
            if (unused) {
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
            // Ignore events from live objects waiting their turn to seed.
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

    _registerEventAsSeen(event: SpecEvent, subjects: string[]) {
        subjects.forEach((subject) => {
            this.seenEvents.set(`${event.id}:${subject}`, true)
        })
    }

    _wasEventSeenByAll(eventId: string, subjects: string[]): boolean {
        for (let subject of subjects) {
            if (!this.seenEvents.has(`${eventId}:${subject}`)) {
                return false
            }
        }
        return true
    }

    _updateEventCursor(event: SpecEvent) {
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
