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
    Trigger,
    TriggerEvent,
    TriggerProcedure,
    ReorgSub,
    ReorgEvent,
    EventCursor,
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
    upsertOpTrackingEntries,
    freezeTablesForChainId,
    deleteOpsOlderThan,
} from './lib/db/spec'
import { SpecEvent } from '@spec.dev/event-client'
import LRU from 'lru-cache'
import ApplyEventService from './lib/services/ApplyEventService'
import UpsertLiveColumnsService from './lib/services/UpsertLiveColumnsService'
import SeedTableService from './lib/services/SeedTableService'
import RollbackService from './lib/services/RollbackService'
import { tableSubscriber } from './lib/db/subscriber'
import short from 'short-uuid'
import ResolveRecordsService from './lib/services/ResolveRecordsService'
import { getRecordsForPrimaryKeys } from './lib/db'
import { importHooks } from './lib/hooks'
import { sleep } from './lib/utils/time'
import { unique, stringify, mapBy } from './lib/utils/formatters'
import chalk from 'chalk'
import { hash } from './lib/utils/hash'
import { tablesMeta } from './lib/db/tablesMeta'
import { db } from './lib/db'
import { getSpecTriggers, createTrigger, maybeDropTrigger } from './lib/db/triggers'
import { importHandlers, getHandlers, CUSTOM_EVENT_HANDLER_KEY } from './lib/handlers'
import { subtractMinutes } from './lib/utils/time'

class Spec {
    liveObjects: { [key: string]: LiveObject } = {}

    eventSubs: { [key: string]: EventSub } = {}

    reorgSubs: { [key: string]: ReorgSub } = {}

    customEventHandlers: { [key: string]: any } = {}

    cleanupOpsJob: any = null

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
        await tableSubscriber.upsertTableSubs()

        // Connect to event/rpc message client.
        // Force run the onConnect handler if already connected.
        messageClient.client ? messageClient.onConnect() : messageClient.connect()
    }

    async _onMessageClientConnected() {
        // Resolve all live objects for the versions listed in the config file.
        await this._getLiveObjectsInConfig()

        // Kick start op-tracking.
        await Promise.all([this._upsertOpTrackingTriggers(), this._upsertOpTrackingEntries()])
        this._upsertCleanupOpsJob()

        // Upsert live columns listed in the config and start seeding new ones.
        if (Object.keys(this.liveObjects).length) {
            await this._upsertAndSeedLiveColumns()
        }

        // Subscribe to reorg events for the chains needed.
        await this._subscribeToReorgs()

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
        this._upsertSaveCursorsJob()
        this._doneProcessingNewConfig()
    }

    async _onEvent(event: SpecEvent, options?: StringKeyMap) {
        // Ensure we're actually subscribed to this event.
        const sub = this.eventSubs[event.name]
        if (!sub) {
            logger.error(
                chalk.redBright(
                    `Got event for ${event.name} without subscription...something's wrong.`
                )
            )
            return
        }

        // Detect & fill in any gaps in events by hashing ids in series.
        const currentLastValue = { ...(sub.last || {}) }
        const gapDetected = currentLastValue.id && hash(currentLastValue.id) !== event.id
        const newLastValue = {
            id: event.id,
            nonce: event.nonce,
            name: event.name,
            timestamp: event.origin.eventTimestamp,
            blockNumber: event.origin.blockNumber,
        }
        if (gapDetected && !sub.isGapFilling) {
            this.eventSubs[event.name].isGapFilling = true
            this.eventSubs[event.name].shouldBuffer = true
            this.eventSubs[event.name].last = newLastValue
            logger.warn(
                chalk.magenta(
                    `Gap in "${event.name}" detected [${currentLastValue.blockNumber} -> ${newLastValue.blockNumber}]\n` +
                        `Patching from ${currentLastValue.id}...`
                )
            )
            await this._fetchEventsAfter(currentLastValue)
            await this._processAllBufferedEvents([event.name])
            return
        }

        if (sub.isGapFilling && !options?.forceToBuffer) {
            // New events coming while actively filling gaps but that aren't
            // associated with the gap. Guards against potential race condition.
            this.eventSubs[event.name].setToLastAfterFillingGaps = newLastValue
        } else {
            this.eventSubs[event.name].last = newLastValue
        }

        // Ensure at least one live object (or custom handler) will process this event.
        const hasCustomEventHandler = this.customEventHandlers.hasOwnProperty(event.name)
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

        ;(await this._processEvent(event)) && this._updateEventCursor(event)
    }

    async _processEvent(event: SpecEvent): Promise<boolean> {
        // Ignore invalid events associated with any since-fixed reorgs.
        if ((event.origin as StringKeyMap).invalid) return false

        // Get sub for event.
        const sub = this.eventSubs[event.name]
        if (!sub) {
            logger.error(
                `Processing event for ${event.name} without subscription...something's wrong.`
            )
            return false
        }

        // Apply the event to each live object that depends on it.
        let processedEvent = false
        let allTablesFrozen = true
        const liveObjectIds = sub.liveObjectIds || []
        for (const liveObjectId of liveObjectIds) {
            if (this.liveObjectsToIgnoreEventsFrom.has(liveObjectId)) continue

            const liveObject = this.liveObjects[liveObjectId]
            if (!liveObject) continue

            processedEvent = true
            try {
                const service = new ApplyEventService(event, liveObject)
                await service.perform()
                if (!service.allTablesFrozen) {
                    allTablesFrozen = false
                }
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

        const shouldRegisterWithCursor =
            !liveObjectIds.length || !processedEvent || !allTablesFrozen

        return shouldRegisterWithCursor
    }

    async _onReorg(event: ReorgEvent) {
        // Ensure we're actually subscribed to a chain's reorgs.
        if (!this.reorgSubs[event.chainId]) {
            logger.error(
                `Got reorg event for chain ${event.chainId} without subscription...something's wrong.`
            )
            return
        }
        this.reorgSubs[event.chainId].buffer.push(event)

        // Confirm this reorg actually occurred.
        const isValid = await messageClient.validateReorg(event)
        if (!isValid) {
            this.reorgSubs[event.chainId].buffer = this.reorgSubs[event.chainId].buffer.filter(
                (e) => e.id !== event.id
            )
            return
        }

        // Force all live object events to buffer
        // so they don't interfere with the reorg.
        this._bufferAllLiveObjectEvents()
        await sleep(100)

        // Remove any events in the event buffer that are associated
        // with block numbers >= the reorg rollback target.
        this._applyReorgToEventBuffer(event)

        // Process the reorg events for this chain in order.
        if (!this.reorgSubs[event.chainId].isProcessing) {
            this.reorgSubs[event.chainId].isProcessing = true
            await this._processReorg(event.chainId)
        }
    }

    async _processReorg(chainId: string) {
        const reorgEvent = this.reorgSubs[chainId].buffer.shift()
        const rollbackToBlockNumber = Number(reorgEvent.blockNumber)
        const rollbackEventTsDate = new Date(reorgEvent.eventTimestamp)

        // Process any events still in the buffer that came before the reorg event.
        for (const eventName in this.eventSubs) {
            await this._sortEventBuffer(eventName)
            let event, eventToUpdateCursorWith
            while (this.eventSubs[eventName].buffer.length) {
                const eventTsDate = new Date(
                    this.eventSubs[eventName].buffer[0].origin.eventTimestamp
                )
                if (eventTsDate > rollbackEventTsDate) break
                event = this.eventSubs[eventName].buffer.shift()
                if (await this._processEvent(event)) {
                    eventToUpdateCursorWith = event
                }
            }
            eventToUpdateCursorWith && this._updateEventCursor(eventToUpdateCursorWith)
        }

        // Roll records back to their latest valid snapshot.
        const service = new RollbackService(rollbackToBlockNumber, chainId)
        try {
            await service.perform()
        } catch (err) {
            logger.error(
                chalk.redBright(
                    `[${chainId}:${rollbackToBlockNumber}] Reorg failed: ${stringify(err)}`
                )
            )
            await freezeTablesForChainId(service.tablePaths, chainId)
        }

        // Keep pulling from the buffer until all reorgs are complete.
        if (this.reorgSubs[chainId].buffer.length) {
            await this._processReorg(chainId)
            return
        }

        // Mark as done and process any new events
        // received while the reorg was running.
        this.reorgSubs[chainId].isProcessing = false
        const eventsNotFillingGaps = []
        for (const eventName in this.eventSubs) {
            if (this.eventSubs[eventName].isGapFilling) continue
            eventsNotFillingGaps.push(eventName)
        }
        await this._processAllBufferedEvents(eventsNotFillingGaps)
    }

    async _getLiveObjectsInConfig() {
        const liveObjects = await resolveLiveObjects(config.liveObjectIds, this.liveObjects)
        for (const liveObject of liveObjects) {
            this.liveObjects[liveObject.id] = liveObject
        }
    }

    async _subscribeToReorgs() {
        const chainIds = this._getCurrentlyUsedChainIds()
        if (!chainIds.length) return

        for (const chainId of chainIds) {
            if (this.reorgSubs.hasOwnProperty(chainId)) continue

            this.reorgSubs[chainId] = {
                chainId,
                isProcessing: false,
                buffer: [],
            }

            messageClient.on(this._formatReorgEventName(chainId), (event) =>
                this._onReorg(event as unknown as ReorgEvent)
            )
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
            messageClient.on(newEventName, (event: SpecEvent) =>
                this._onEvent(event).catch((e) => logger.error(e))
            )

            // Register sub.
            this.eventSubs[newEventName] = {
                name: newEventName,
                liveObjectIds: liveObjectsByEvent[newEventName],
                cursor: null,
                cursorChanged: false,
                shouldBuffer: true,
                buffer: [],
                last: null,
                isGapFilling: false,
                setToLastAfterFillingGaps: null,
            }

            newEventNames.push(newEventName)
        }

        // Unsubscribe from events that aren't needed anymore.
        this._removeUselessSubs(liveObjectsByEvent)

        // Subscribe to events for custom handlers.
        for (const eventName in this.customEventHandlers) {
            if (this.eventSubs.hasOwnProperty(eventName)) continue
            messageClient.on(eventName, (event: SpecEvent) =>
                this._onEvent(event).catch((e) => logger.error(e))
            )

            // Register sub with no live object ids.
            this.eventSubs[eventName] = {
                name: eventName,
                liveObjectIds: [],
                cursor: null,
                cursorChanged: false,
                shouldBuffer: true,
                buffer: [],
                last: null,
                isGapFilling: false,
                setToLastAfterFillingGaps: null,
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
        const eventNamesToBuffer = []
        for (const newEventName of newEventNames) {
            const cursor = this.eventSubs[newEventName].cursor
            if (!cursor) continue
            cursors.push(cursor)
            eventNamesToBuffer.push(newEventName)
        }
        if (!cursors.length) return

        eventNamesToBuffer.forEach((eventName) => {
            this.eventSubs[eventName].shouldBuffer = true
            this.eventSubs[eventName].isGapFilling = true
        })

        logger.info('Fetching any missed events...')

        return new Promise(async (res, _) => {
            try {
                const promises = []
                await messageClient.fetchMissedEvents(
                    cursors,
                    async (events: SpecEvent[]) => {
                        const handleEvents = async () => {
                            logger.info(chalk.cyanBright(`Fetched ${events.length} missed events.`))
                            for (const event of events) {
                                await this._onEvent(event, { forceToBuffer: true })
                            }
                        }
                        promises.push(handleEvents())
                    },
                    async () => {
                        await Promise.all(promises)
                        logger.info(chalk.cyanBright(`Events in sync.`))
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

    // TODO: Consolidate with above.
    async _fetchEventsAfter(cursor: StringKeyMap) {
        return new Promise(async (res, _) => {
            try {
                const promises = []
                let i = 0
                await messageClient.fetchMissedEvents(
                    [cursor as EventCursor],
                    async (events: SpecEvent[]) => {
                        const handleEvents = async () => {
                            i += events.length
                            for (const event of events) {
                                await this._onEvent(event, { forceToBuffer: true })
                            }
                        }
                        promises.push(handleEvents())
                    },
                    async () => {
                        logger.info(chalk.cyanBright(`Patched event gap with ${i} events.`))
                        await Promise.all(promises)
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
            const tableChainInfo = config.getChainInfoForTable(tablePath, this.liveObjects)
            if (!tableChainInfo) continue
            const { liveObjectChainIds } = tableChainInfo

            const isReorgActivelyProcessing = () => {
                for (const chainId of liveObjectChainIds) {
                    if (this.reorgSubs[chainId]?.isProcessing) {
                        return true
                    }
                }
                return false
            }

            try {
                const seedTableService = new SeedTableService(
                    seedSpec,
                    liveObject,
                    seedCursor.id,
                    seedCursor.cursor,
                    seedCursor.metadata,
                    true, // updateOpTrackingFloorAsSeedProgresses
                    liveObjectChainIds,
                    isReorgActivelyProcessing
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
        this._upsertRetrySeedCursorsJob()
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
        const { liveObjectId, tablePath } = seedSpec

        const liveObject = this.liveObjects[liveObjectId]
        if (!liveObject) {
            logger.error(
                `Live object ${liveObjectId} isn't registered anymore - skipping seed cursor in series.`,
                seedCursor
            )
            // Register success anyway and go to next in series.
            await seedSucceeded(seedCursor.id)
            seedCursor.metadata?.nextId &&
                this._runNextSeedCursorInSeries(seedCursor.metadata?.nextId)
            return
        }

        const tableChainInfo = config.getChainInfoForTable(tablePath, this.liveObjects)
        if (!tableChainInfo) {
            logger.error(
                `Live object chain info not found - skipping seed cursor in series.`,
                seedCursor
            )
            // Register success anyway and go to next in series.
            await seedSucceeded(seedCursor.id)
            seedCursor.metadata?.nextId &&
                this._runNextSeedCursorInSeries(seedCursor.metadata?.nextId)
            return
        }
        const { liveObjectChainIds } = tableChainInfo
        const isReorgActivelyProcessing = () => {
            for (const chainId of liveObjectChainIds) {
                if (this.reorgSubs[chainId]?.isProcessing) {
                    return true
                }
            }
            return false
        }

        let seedTableService: SeedTableService
        try {
            seedTableService = new SeedTableService(
                seedSpec,
                liveObject,
                seedCursor.id,
                seedCursor.cursor,
                seedCursor.metadata,
                true, // updateOpTrackingFloorAsSeedProgresses
                liveObjectChainIds,
                isReorgActivelyProcessing
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
        this._upsertSaveCursorsJob()
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
        let event, eventToUpdateCursorWith
        while (this.eventSubs[eventName].buffer.length) {
            event = this.eventSubs[eventName].buffer.shift()
            if (await this._processEvent(event)) {
                eventToUpdateCursorWith = event
            }
            await sleep(5)
        }

        const targetLastValue = this.eventSubs[eventName].setToLastAfterFillingGaps
        const currentLastValue = this.eventSubs[eventName].last
        if (targetLastValue) {
            const targetLastTs = new Date(targetLastValue.timestamp)
            const currentLastTs = currentLastValue ? new Date(currentLastValue.timestamp) : null

            if (!currentLastTs || (currentLastTs && targetLastTs > currentLastTs)) {
                this.eventSubs[eventName].last = targetLastValue
            }

            this.eventSubs[eventName].setToLastAfterFillingGaps = null
        }

        // Update cursors and re-enable active processing.
        this.eventSubs[eventName].shouldBuffer = false
        this.eventSubs[eventName].isGapFilling = false
        eventToUpdateCursorWith && this._updateEventCursor(eventToUpdateCursorWith)
    }

    _applyReorgToEventBuffer(reorgEvent: ReorgEvent) {
        const rollbackToBlockNumber = Number(reorgEvent.blockNumber)
        const rollbackEventTsDate = new Date(reorgEvent.eventTimestamp)

        // Remove any events from the live object event buffer that are:
        // 1) >= rollback block number (assuming chain id is equivalent)
        // 2) were sent before the rollback event
        for (const eventName in this.eventSubs) {
            const invalidEventIds = new Set<string>()
            for (const event of this.eventSubs[eventName].buffer) {
                if (event.origin.chainId !== reorgEvent.chainId) continue
                const eventBlockNumber = Number(event.origin.blockNumber)
                const eventTsDate = new Date(event.origin.eventTimestamp)
                if (
                    eventBlockNumber >= rollbackToBlockNumber &&
                    eventTsDate < rollbackEventTsDate
                ) {
                    invalidEventIds.add(event.id)
                }
            }
            this.eventSubs[eventName].buffer = this.eventSubs[eventName].buffer.filter(
                (event) => !invalidEventIds.has(event.id)
            )
        }
    }

    async _sortEventBuffer(eventName: string): Promise<void> {
        return new Promise(async (res, _) => {
            while (true) {
                const buffer = this.eventSubs[eventName].buffer
                if (!buffer.length) break

                // Dedupe and sort the buffer oldest to newest.
                const deduped = Object.values(mapBy([...buffer], 'id'))
                const sortedBuffer = deduped.sort((a, b) => {
                    return (
                        parseFloat(a.nonce.replace('-', '.')) -
                        parseFloat(b.nonce.replace('-', '.'))
                    )
                })

                // Try again if some new event was buffered (race condition) during ^this sort.
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

    async _upsertOpTrackingTriggers() {
        const tablePaths = config.getAllReferencedTablePaths()

        // Get all existing spec op-tracking triggers.
        let triggers
        try {
            triggers = await getSpecTriggers(TriggerProcedure.TrackOps)
        } catch (err) {
            logger.error(`Failed to fetch op-tracking spec triggers: ${err}`)
            return
        }

        // Map existing triggers by <schema>:<table>:<event>
        const existingTriggersMap = {}
        for (const trigger of triggers) {
            const { schema, table, event } = trigger
            const key = [schema, table, event].join(':')
            existingTriggersMap[key] = trigger
        }

        // Upsert triggers for each table referenced in the project config.
        await Promise.all(
            tablePaths.map((tablePath) =>
                this._upsertOpTrackingTrigger(tablePath, existingTriggersMap)
            )
        )
    }

    async _upsertOpTrackingTrigger(
        tablePath: string,
        existingTriggersMap: { [key: string]: Trigger }
    ) {
        const [schema, table] = tablePath.split('.')
        const currentPrimaryKeyCols = tablesMeta[tablePath].primaryKey
        const currentPrimaryKeys = currentPrimaryKeyCols.map((pk) => pk.name)

        // Get the current spec triggers for this table.
        const insertTriggerKey = [schema, table, TriggerEvent.INSERT].join(':')
        const updateTriggerKey = [schema, table, TriggerEvent.UPDATE].join(':')
        const insertTrigger = existingTriggersMap[insertTriggerKey]
        const updateTrigger = existingTriggersMap[updateTriggerKey]

        // Should create new triggers if they don't exist.
        let createInsertTrigger = !insertTrigger
        let createUpdateTrigger = !updateTrigger

        // If any of the triggers already exist, ensure the primary keys haven't changed.
        if (insertTrigger) {
            createInsertTrigger = await maybeDropTrigger(
                insertTrigger,
                schema,
                table,
                TriggerProcedure.TrackOps,
                currentPrimaryKeys
            )
        }
        if (updateTrigger) {
            createUpdateTrigger = await maybeDropTrigger(
                updateTrigger,
                schema,
                table,
                TriggerProcedure.TrackOps,
                currentPrimaryKeys
            )
        }
        if (!createInsertTrigger && !createUpdateTrigger) return true

        // Create the missing triggers.
        const promises = []
        createInsertTrigger &&
            promises.push(
                createTrigger(
                    schema,
                    table,
                    TriggerEvent.INSERT,
                    TriggerProcedure.TrackOps,
                    this.liveObjects
                )
            )
        createUpdateTrigger &&
            promises.push(
                createTrigger(
                    schema,
                    table,
                    TriggerEvent.UPDATE,
                    TriggerProcedure.TrackOps,
                    this.liveObjects
                )
            )
        try {
            await Promise.all(promises)
        } catch (err) {
            logger.error(`Error creating op-tracking triggers for ${tablePath}: ${err}`)
            return false
        }

        return true
    }

    async _upsertOpTrackingEntries() {
        const opTrackingEntries = []
        for (const tablePath of config.getAllReferencedTablePaths()) {
            const tableChainInfo = config.getChainInfoForTable(tablePath, this.liveObjects)
            if (!tableChainInfo) return false
            for (const chainId of tableChainInfo.liveObjectChainIds) {
                opTrackingEntries.push({
                    tablePath,
                    chainId,
                    isEnabledAbove: 0,
                })
            }
        }
        opTrackingEntries.length && (await upsertOpTrackingEntries(opTrackingEntries, false))
    }

    _doneProcessingNewConfig() {
        if (this.hasPendingConfigUpdate) {
            this.hasPendingConfigUpdate = false
            this._onNewConfig()
            return
        }
        this.isProcessingNewConfig = false
    }

    _upsertCleanupOpsJob() {
        this.cleanupOpsJob =
            this.cleanupOpsJob ||
            setInterval(() => {
                deleteOpsOlderThan(subtractMinutes(new Date(), constants.CLEANUP_OPS_OLDER_THAN))
            }, constants.CLEANUP_OPS_INTERVAL)
    }

    _upsertSaveCursorsJob() {
        this.saveEventCursorsJob =
            this.saveEventCursorsJob ||
            setInterval(() => this._saveEventCursors(), constants.SAVE_EVENT_CURSORS_INTERVAL)
    }

    _upsertRetrySeedCursorsJob() {
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

    _getCurrentlyUsedChainIds(): string[] {
        const chainIds = new Set<string>()
        Object.values(this.liveObjects).forEach((liveObject) => {
            Object.keys(liveObject.config?.chains || {}).forEach((chainId) => {
                chainIds.add(chainId)
            })
        })
        return Array.from(chainIds)
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

    _bufferAllLiveObjectEvents() {
        for (const eventName in this.eventSubs) {
            this.eventSubs[eventName].shouldBuffer = true
        }
    }

    _formatReorgEventName(chainId: string): string {
        return [constants.REORG_EVENT_NAME_PREFIX, chainId].join(':')
    }
}

export default Spec
