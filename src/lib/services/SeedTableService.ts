import logger from '../logger'
import {
    SeedSpec,
    StringMap,
    LiveObject,
    StringKeyMap,
    TableDataSources,
    OpType,
    ForeignKeyConstraint,
    Filter,
    FilterOp,
    ColumnDefaultsConfig,
    EnrichedLink,
    SelectOptions,
    OrderByDirection,
} from '../types'
import {
    reverseMap,
    getCombinations,
    unique,
    groupByKeys,
    fromNamespacedVersion,
    sortChainIds,
} from '../utils/formatters'
import { areColumnsEmpty } from '../db'
import RunOpService from './RunOpService'
import { querySharedTable } from '../shared-tables/client'
import config from '../config'
import { db } from '../db'
import { QueryError } from '../errors'
import { constants } from '../constants'
import { tablesMeta, getRel, isColTypeArray } from '../db/tablesMeta'
import chalk from 'chalk'
import { updateCursor, updateMetadata, upsertOpTrackingEntries } from '../db/spec'
import LRU from 'lru-cache'
import { withDeadlockProtection } from '../utils/db'
import { isContractNamespace, isPrimitiveNamespace } from '../utils/chains'
import messageClient from '../rpcs/messageClient'

const valueSep = '__:__'

class SeedTableService {
    seedSpec: SeedSpec

    liveObject: LiveObject

    seedCursorId: string

    cursor: number

    metadata: StringKeyMap

    updateOpTrackingFloorAsSeedProgresses: boolean

    liveObjectChainIds: string[]

    isReorgActivelyProcessing: Function | null

    seedColNames: Set<string>

    requiredArgColPaths: string[] = []

    colPathsToFunctionInputArgs: StringKeyMap[] = []

    seedCount: number = 0

    inputBatchSeedCount: number = 0

    fromScratchBatchResultSize: number = 0

    tableDataSources: TableDataSources

    liveTableColumns: string[]

    defaultColumnValues: { [key: string]: ColumnDefaultsConfig }

    seedWithColPaths: StringMap[]

    primaryTimestampColumn: string | null

    enrichedLink: EnrichedLink

    valueFilters: { [key: string]: Filter }[]

    seedStrategy: () => void | null

    foreignKeyMappings: LRU<string, any> = new LRU({ max: 5000 })

    lastOpTrackingFloorUpdate: Date | null = null

    get seedTablePath(): string {
        return this.seedSpec.tablePath
    }

    get seedSchemaName(): string {
        return this.seedTablePath.split('.')[0]
    }

    get seedTableName(): string {
        return this.seedTablePath.split('.')[1]
    }

    get linkProperties(): StringMap {
        return this.enrichedLink.linkOn || {}
    }

    get reverseLinkProperties(): StringMap {
        return reverseMap(this.enrichedLink.linkOn)
    }

    get seedTablePrimaryKeys(): string[] {
        const meta = tablesMeta[this.seedTablePath]
        if (!meta) throw `No meta registered for table ${this.seedTablePath}`
        return meta.primaryKey.map((pk) => pk.name)
    }

    get primaryTimestampProperty(): string | null {
        return this.liveObject.config?.primaryTimestampProperty || null
    }

    get sharedTablePath(): string {
        return this.liveObject.config.table
    }

    get attemptOpTrackingFloorUpdate(): boolean {
        return (
            this.updateOpTrackingFloorAsSeedProgresses &&
            this.isReorgActivelyProcessing &&
            !this.isReorgActivelyProcessing()
        )
    }

    get liveObjectName(): string {
        return fromNamespacedVersion(this.liveObject.id).name
    }

    get liveObjectHasContractNsp(): boolean {
        return isContractNamespace(fromNamespacedVersion(this.liveObject.id).nsp)
    }

    get liveObjectHasSpecNsp(): boolean {
        return fromNamespacedVersion(this.liveObject.id).nsp === constants.SPEC
    }

    constructor(
        seedSpec: SeedSpec,
        liveObject: LiveObject,
        seedCursorId: string,
        cursor: number,
        metadata?: StringKeyMap | null,
        updateOpTrackingFloorAsSeedProgresses?: boolean,
        isReorgActivelyProcessing?: Function | null
    ) {
        this.seedSpec = seedSpec
        this.liveObject = liveObject
        this.seedCursorId = seedCursorId
        this.cursor = cursor || 0
        this.metadata = metadata || {}
        this.updateOpTrackingFloorAsSeedProgresses = !!updateOpTrackingFloorAsSeedProgresses
        this.isReorgActivelyProcessing = isReorgActivelyProcessing || null

        this.liveObjectChainIds = sortChainIds(Object.keys(liveObject.config?.chains || {}))
        if (!this.liveObjectChainIds.length) {
            throw `No chain ids associated with ${this.liveObject.id}`
        }

        this.seedColNames = new Set<string>(this.seedSpec.seedColNames)
        this.tableDataSources = config.getLiveObjectTableDataSources(
            this.liveObject.id,
            this.seedTablePath
        )
        this.defaultColumnValues = config.getDefaultColumnValuesForTable(this.seedTablePath)

        this.enrichedLink = config.getEnrichedLink(this.liveObject.id, this.seedTablePath)
        if (!this.enrichedLink) {
            throw `No enriched link found for link ${this.liveObject.id} <> ${this.seedTablePath}`
        }

        this.liveTableColumns = Object.keys(
            config.getTable(this.seedSchemaName, this.seedTableName) || {}
        )
        this.primaryTimestampColumn = this.primaryTimestampProperty
            ? (this.tableDataSources[this.primaryTimestampProperty] || [])[0]?.columnName || null
            : null

        this.seedStrategy = null
    }

    async perform() {
        await this.determineSeedStrategy()
        await this.executeSeedStrategy()
    }

    async determineSeedStrategy() {
        this._findRequiredArgColumns()

        const inputTableColumns = {}
        let numForeignTableSeedColumns = 0
        const uniqueTablePaths = new Set<string>()
        const uniqueRequiredArgColPaths = unique(this.requiredArgColPaths.flat())

        for (const colPath of uniqueRequiredArgColPaths) {
            const [schemaName, tableName, colName] = colPath.split('.')
            const tablePath = [schemaName, tableName].join('.')
            uniqueTablePaths.add(tablePath)

            if (tablePath === this.seedTablePath) {
                throw `[${this.seedTablePath}] Can't seed a table using its own columns`
            }
            numForeignTableSeedColumns++

            if (!inputTableColumns.hasOwnProperty(tablePath)) {
                inputTableColumns[tablePath] = []
            }
            inputTableColumns[tablePath].push(colName)
        }

        const promises = []
        for (const tablePath in inputTableColumns) {
            const colNames = inputTableColumns[tablePath]
            promises.push(areColumnsEmpty(tablePath, colNames))
        }
        const colsEmptyResults = await Promise.all(promises)
        const allRequiredInputColsAreEmpty = colsEmptyResults.filter((v) => !v).length === 0

        // Potentially seed completely from scratch.
        if (allRequiredInputColsAreEmpty) {
            if (numForeignTableSeedColumns > 0) {
                logger.warn(
                    `${this.seedTablePath} - Can't seed a cross-table relationship from scratch.`
                )
                return
            }
            this.seedStrategy = async () => await this._seedFromScratch()
        }
        // TODO: This is possible in the future, just gonna take a lot more lookup work...
        else if (numForeignTableSeedColumns > 1 && uniqueTablePaths.size > 1) {
            throw `[${this.seedTablePath}] Can't yet seed tables using exclusively more than one foreign table.`
        }
        // Seed using the existing records of a foreign table.
        else {
            const tablePath = Object.keys(inputTableColumns)[0] as string
            const colNames = inputTableColumns[tablePath] as string[]
            this.seedStrategy = async () => await this._seedWithForeignTable(tablePath, colNames)
        }
    }

    async executeSeedStrategy() {
        this.seedStrategy && (await this.seedStrategy())
    }

    async seedWithForeignRecords(foreignTablePath: string, records: StringKeyMap[]) {
        this._findRequiredArgColumns()
        if (!this.requiredArgColPaths.length) throw 'No required arg column paths found.'

        // Get input column names and ensure all belong to the given foreign table.
        const inputColNames = []
        const uniqueRequiredArgColPaths = unique(this.requiredArgColPaths.flat())
        for (const colPath of uniqueRequiredArgColPaths) {
            const [schemaName, tableName, colName] = colPath.split('.')
            const tablePath = [schemaName, tableName].join('.')

            if (tablePath !== foreignTablePath) {
                const err = `seedWithForeignRecords can only be used if all 
                required arg cols exist on the given foreign table: ${foreignTablePath}; 
                requiredArgColPaths: ${uniqueRequiredArgColPaths}.`
                throw err
            }
            inputColNames.push(colName)
        }

        if (!inputColNames.length) {
            logger.info(`Not seeding ${foreignTablePath} -- inputColNames came up empty.`)
        }

        // Only act on records where all input columns are non-empty.
        const filteredRecords = []
        for (const record of records) {
            let ignoreRecord = false
            for (const inputColName of inputColNames) {
                const val = record[inputColName]
                if (val === null || (Array.isArray(val) && !val.length)) {
                    ignoreRecord = true
                    break
                }
            }
            if (ignoreRecord) continue
            filteredRecords.push(record)
        }
        if (!filteredRecords.length) return

        // Seed with the explicitly given foreign input records.
        await this._seedWithForeignTable(foreignTablePath, inputColNames, filteredRecords)
    }

    async _seedFromScratch() {
        logger.info(chalk.cyanBright(`\nSeeding ${this.seedTablePath} from scratch...`))

        this.attemptOpTrackingFloorUpdate && (await this._updateOpTrackingFloor())
        const allInputArgs = this.valueFilters.map((vf) =>
            this._formatValueFilterGroupAsInputArgs(vf)
        )
        const inputArgsByChainId = {}

        let useSeekMethod = false
        let dataSourceStartBlocks = {}
        let currentChainHeads = {}
        let chainIdsToSource: string[] = [''] // empty string to force a single iteration at seed-time

        const seedOneChainAtATime = this.liveObjectHasContractNsp || this.liveObjectHasSpecNsp
        if (seedOneChainAtATime) {
            const filtersExist = !!allInputArgs.length
            let chainIdsWithData = new Set(this.liveObjectChainIds) // start off assuming all

            // Contract event or transactions.
            if (this.liveObjectHasContractNsp) {
                // Make preflight request to get block range & record count for data backing this live table.
                const { data: preflightInfo, error } = await messageClient.getSeedPreflightInfo(
                    this.liveObject.id,
                    this.sharedTablePath
                )
                if (error)
                    throw `[${this.seedTablePath}] Error performing preflight seed request: ${error}`

                dataSourceStartBlocks = preflightInfo.startBlocks || {}
                currentChainHeads = preflightInfo.heads || {}
                for (const key in currentChainHeads) {
                    currentChainHeads[key] = Number(currentChainHeads[key])
                }
                chainIdsWithData = new Set(Object.keys(dataSourceStartBlocks))

                const { recordCount } = preflightInfo
                if (
                    recordCount === null ||
                    recordCount > constants.EVENT_OFFSET_LIMIT_SEED_THRESHOLD
                ) {
                    useSeekMethod = true
                }
            }

            chainIdsToSource = sortChainIds(Array.from(chainIdsWithData))

            if (filtersExist) {
                // For each inclusive group of AND filters...
                for (const inputArg of allInputArgs) {
                    const otherFilters = {}

                    // Find the chain id filter (if any).
                    let chainIdFilter
                    for (const [key, value] of Object.entries(inputArg)) {
                        if (key === 'chainId') {
                            chainIdFilter = value
                        } else {
                            otherFilters[key] = value
                        }
                    }

                    // If no chain id filter is given for this group, assign the
                    // "other" filters to all chain ids with to source (with data).
                    const hasOtherFilters = Object.keys(otherFilters).length > 0
                    if (!chainIdFilter && hasOtherFilters) {
                        for (const chainId of chainIdsToSource) {
                            inputArgsByChainId[chainId] = inputArgsByChainId[chainId] || []
                            inputArgsByChainId[chainId].push(otherFilters)
                        }
                        continue
                    }

                    const filterType = typeof chainIdFilter
                    let chainIdValues =
                        filterType === 'object' &&
                        chainIdFilter.op === FilterOp.In &&
                        Array.isArray(chainIdFilter.value)
                            ? chainIdFilter.value
                            : [chainIdFilter]

                    chainIdValues = chainIdValues
                        .filter(
                            (chainId) =>
                                (typeof chainId === 'string' || typeof chainId === 'number') &&
                                !!chainId
                        )
                        .map((chainId) => chainId.toString())

                    chainIdValues.forEach((chainId) => {
                        inputArgsByChainId[chainId] = inputArgsByChainId[chainId] || []
                        hasOtherFilters && inputArgsByChainId[chainId].push(otherFilters)
                    })
                }

                const chainIdsFromFilters = Object.keys(inputArgsByChainId)
                chainIdsToSource = sortChainIds(
                    chainIdsFromFilters.filter((chainId) => chainIdsWithData.has(chainId))
                )
            }
        }

        const chainIds = this.metadata.chainIds || chainIdsToSource
        const initialChainIndex = this.metadata.chainIndex || 0
        const sharedErrorContext = { error: null }
        const t0 = performance.now()

        for (let i = initialChainIndex; i < chainIds.length; i++) {
            const chainId = chainIds[i]
            const inputArgs = chainId ? inputArgsByChainId[chainId] || [] : allInputArgs
            const currentChainHead = currentChainHeads[chainId]
            const isFirstIteration = i === initialChainIndex

            if (useSeekMethod) {
                if (isFirstIteration) {
                    this.cursor = this.cursor || dataSourceStartBlocks[chainId] || 0
                } else {
                    this.cursor = dataSourceStartBlocks[chainId] || 0
                }
            } else {
                if (isFirstIteration) {
                    this.cursor = this.cursor || 0 // assume potentially saved cursor from a previous failure
                } else {
                    this.cursor = 0 // always 0 on iterations after the first
                }
            }

            const options: SelectOptions = {}
            if (chainId) {
                options.chainId = chainId
                await updateMetadata(this.seedCursorId, {
                    ...this.metadata,
                    chainIds,
                    chainIndex: i,
                })
            }

            if (!useSeekMethod) {
                options.limit = constants.FROM_SCRATCH_SEED_INPUT_BATCH_SIZE
                options.orderBy = {
                    column: this.primaryTimestampProperty,
                    direction: OrderByDirection.ASC,
                }
            }

            while (true) {
                if (useSeekMethod) {
                    const fromBlock = this.cursor
                    const toBlock = this.cursor + constants.SEEK_BLOCK_RANGE_SIZE
                    options.blockRange = [fromBlock]

                    // Leave "to" off of the last query.
                    if (
                        toBlock < currentChainHead &&
                        currentChainHead - toBlock > constants.SEEK_BLOCK_RANGE_SIZE * 0.3
                    ) {
                        options.blockRange.push(toBlock)
                    }
                } else {
                    options.offset = this.cursor
                }

                this.fromScratchBatchResultSize = 0
                try {
                    await querySharedTable(
                        this.sharedTablePath,
                        inputArgs,
                        async (data) => {
                            return this._handleDataOnSeedFromScratch(data as StringKeyMap[]).catch(
                                (err) => {
                                    sharedErrorContext.error = err
                                }
                            )
                        },
                        sharedErrorContext,
                        options,
                        this.metadata.fromTrigger
                    )
                } catch (err) {
                    logger.error(err)
                    throw err
                }
                if (sharedErrorContext.error) throw sharedErrorContext.error

                let isLastBatch = false
                if (useSeekMethod) {
                    if (options.blockRange.length > 1) {
                        this.cursor = options.blockRange[1] + 1
                    } else {
                        isLastBatch = true
                    }
                } else {
                    this.cursor += this.fromScratchBatchResultSize
                    isLastBatch = this.fromScratchBatchResultSize < options.limit
                }

                await updateCursor(this.seedCursorId, this.cursor)

                if (!isLastBatch && this.attemptOpTrackingFloorUpdate) {
                    await this._updateOpTrackingFloor()
                }

                if (isLastBatch) break
            }
        }

        const tf = performance.now()
        const seconds = Number(((tf - t0) / 1000).toFixed(2))
        logger.info(chalk.cyanBright('Done.'))
        logger.info(
            chalk.cyanBright(
                `Upserted ${this.seedCount.toLocaleString('en-US')} records in ${seconds} seconds.`
            )
        )
    }

    async _seedWithForeignTable(
        foreignTablePath: string,
        inputColNames: string[],
        inputRecords?: StringKeyMap[]
    ) {
        if (Array.isArray(inputRecords) && !inputRecords.length) return

        const foreignTableName = foreignTablePath.split('.')[1]
        logger.info(
            chalk.cyanBright(
                `\nSeeding ${this.seedTableName} using ${foreignTableName}(${inputColNames.join(
                    ', '
                )})...`
            )
        )

        // Get seed table -> foreign table relationship.
        const rel = getRel(this.seedTablePath, foreignTablePath)
        const foreignTablePrimaryKeys = tablesMeta[foreignTablePath].primaryKey.map((pk) => pk.name)

        // Get the live object property keys associated with each input column.
        const reverseLinkProperties = this.reverseLinkProperties
        const inputPropertyKeys = inputColNames.map((colName) => {
            const foreignInputColPath = [foreignTablePath, colName].join('.')
            let property = reverseLinkProperties[foreignInputColPath]
            if (property) return property

            const propertySet = new Set()
            for (const filterGroup of this.enrichedLink.filterBy) {
                for (const property in filterGroup) {
                    const filter = filterGroup[property]
                    const colPath = filter.column
                    if (colPath === foreignInputColPath && filter.op === FilterOp.EqualTo) {
                        propertySet.add(property)
                    }
                }
            }
            const propertyArray = Array.from(propertySet)
            if (!propertyArray.length) {
                throw `Couldn't find input property associated with input foreign column ${foreignInputColPath}`
            }
            return propertyArray.length > 1 ? propertyArray : propertyArray[0]
        })

        this.attemptOpTrackingFloorUpdate && (await this._updateOpTrackingFloor())
        let seedInputBatchSize = constants.FOREIGN_SEED_INPUT_BATCH_SIZE

        const liveObjectNsp = fromNamespacedVersion(this.liveObject.id).nsp
        if (isPrimitiveNamespace(liveObjectNsp)) {
            seedInputBatchSize = Math.min(seedInputBatchSize, 20)
        }

        const foreignInputColPaths = inputColNames.map((colName) =>
            [foreignTablePath, colName].join('.')
        )
        let chainIdColName = null
        for (const colPathsToFunctionInputArgs of this.colPathsToFunctionInputArgs) {
            for (const colPath of foreignInputColPaths) {
                const inputProperties = (colPathsToFunctionInputArgs[colPath] || []).map(
                    (entry) => entry.property
                )
                if (inputProperties.includes('chainId')) {
                    chainIdColName = colPath.split('.').pop()
                    break
                }
            }
            if (chainIdColName) break
        }

        const sharedErrorContext = { error: null }
        let t0 = null
        while (true) {
            if (sharedErrorContext.error) throw sharedErrorContext.error

            let batchInputRecords
            if (inputRecords) {
                batchInputRecords = inputRecords.slice(
                    this.cursor,
                    this.cursor + seedInputBatchSize
                )
            } else {
                batchInputRecords = await this._getForeignInputRecordsBatch(
                    foreignTablePath,
                    foreignTablePrimaryKeys as string[],
                    inputColNames,
                    this.cursor,
                    seedInputBatchSize
                )
            }

            // Reduce the batch to the first N records that share the same chain id.
            let reducedBatchSize = false
            if (chainIdColName) {
                let chainBatchInputRecords = []
                let batchChainId = batchInputRecords[0][chainIdColName]
                const prevBatchInputRecordsLength = batchInputRecords.length
                for (const inputRecord of batchInputRecords) {
                    if (inputRecord[chainIdColName] !== batchChainId) break
                    chainBatchInputRecords.push(inputRecord)
                }
                batchInputRecords = chainBatchInputRecords
                reducedBatchSize = batchInputRecords.length < prevBatchInputRecordsLength
            }

            if (batchInputRecords === null) {
                throw `Foreign input records batch came up null for ${foreignTablePath} at offset ${this.cursor}.`
            }
            if (!batchInputRecords.length) {
                this._logResults(t0)
                break
            }

            if (batchInputRecords.length > 1) {
                logger.info(
                    chalk.cyanBright(
                        `\n[New input batch of ${batchInputRecords.length} records]:\n`
                    )
                )
            } else {
                const kvs = inputColNames
                    .map((colName) => `${colName}: ${batchInputRecords[0][colName]}`)
                    .join(', ')
                logger.info(chalk.cyanBright(`\n[${kvs}]:\n`))
            }

            this.cursor += batchInputRecords.length
            const isLastBatch = !reducedBatchSize && batchInputRecords.length < seedInputBatchSize

            // Map the input records to their reference key values so that records added
            // to the seed table can easily find/assign their associated foreign keys.
            const referenceKeyValues = {}
            if (rel) {
                for (const record of batchInputRecords) {
                    const colValues = inputColNames.map((colName) => record[colName])
                    const colValueOptions = getCombinations(colValues)
                    for (const valueOptions of colValueOptions) {
                        const key = valueOptions.join(valueSep)
                        referenceKeyValues[key] = referenceKeyValues[key] || []
                        referenceKeyValues[key].push(
                            rel.referenceKey.map((c) => record[c]).join(valueSep)
                        )
                    }
                }
            }

            // Transform the records into seed-function inputs.
            const { batchFunctionInputs, typeConversionMap } =
                this._transformRecordsIntoFunctionInputs(batchInputRecords, foreignTablePath)

            // Callback to use when a batch of response data is available.
            const onFunctionRespData = async (data) =>
                this._handleDataOnForeignTableSeed(
                    data,
                    rel,
                    inputPropertyKeys,
                    referenceKeyValues,
                    typeConversionMap
                ).catch((err) => {
                    sharedErrorContext.error = err
                })

            // Call spec function and handle response data.
            this.inputBatchSeedCount = 0
            t0 = t0 || performance.now()
            try {
                await querySharedTable(
                    this.sharedTablePath,
                    batchFunctionInputs,
                    onFunctionRespData,
                    sharedErrorContext,
                    {},
                    this.metadata.fromTrigger
                )
            } catch (err) {
                logger.error(err)
                throw err
            }

            if (this.inputBatchSeedCount === 0) {
                logger.info(chalk.cyanBright(`  0 records -> ${this.seedTableName}`))
            }

            if (sharedErrorContext.error) throw sharedErrorContext.error
            await updateCursor(this.seedCursorId, this.cursor)

            if (!isLastBatch && this.attemptOpTrackingFloorUpdate) {
                await this._updateOpTrackingFloor()
            }
            if (isLastBatch) {
                this._logResults(t0)
                break
            }
        }
    }

    async _handleDataOnSeedFromScratch(batch: StringKeyMap[]) {
        this.fromScratchBatchResultSize += batch.length
        const tableDataSources = this.tableDataSources
        const insertRecords = []

        for (const liveObjectData of batch) {
            // Format a seed table record for this live object data.
            const insertRecord = {}
            for (const property in liveObjectData) {
                const colsWithThisPropertyAsDataSource = tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    insertRecord[columnName] = value
                }
            }
            insertRecords.push(insertRecord)
        }

        const insertBatchOp = {
            type: OpType.Insert,
            schema: this.seedSchemaName,
            table: this.seedTableName,
            data: insertRecords,
            conflictTargets: this.enrichedLink.uniqueConstraint,
            liveTableColumns: this.liveTableColumns,
            primaryTimestampColumn: this.primaryTimestampColumn,
            defaultColumnValues: this.defaultColumnValues,
        }

        this.seedCount += insertRecords.length

        logger.info(
            chalk.cyanBright(
                `  ${this.seedCount.toLocaleString('en-US')} records -> ${this.seedTableName}`
            )
        )

        const op = async () => {
            try {
                await db.transaction(async (tx) => {
                    await new RunOpService(insertBatchOp, tx).perform()
                })
            } catch (err) {
                throw new QueryError('insert', this.seedSchemaName, this.seedTableName, err)
            }
        }

        await withDeadlockProtection(op)
    }

    async _handleDataOnForeignTableSeed(
        batch: StringKeyMap[],
        rel: ForeignKeyConstraint,
        inputPropertyKeys: any,
        referenceKeyValues: StringKeyMap,
        typeConversionMap: StringKeyMap
    ) {
        this.inputBatchSeedCount += batch.length
        const inputPropertyKeyOptions = getCombinations(inputPropertyKeys)
        const linkProperties = this.linkProperties
        const tableDataSources = this.tableDataSources
        const upsertRecords = []

        for (const liveObjectData of batch) {
            for (const property in liveObjectData) {
                let value = liveObjectData[property]
                if (
                    typeConversionMap.hasOwnProperty(property) &&
                    typeConversionMap[property].hasOwnProperty(value)
                ) {
                    liveObjectData[property] = typeConversionMap[property][value]
                }
            }

            // Format a seed table record for this live object data.
            const upsertRecord: StringKeyMap = {}
            for (const property in liveObjectData) {
                const colsWithThisPropertyAsDataSource = tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    upsertRecord[columnName] = value
                }
            }

            // Ensure all linked properties have values.
            let ignoreData = false
            for (const property in linkProperties) {
                if (!liveObjectData.hasOwnProperty(property) || liveObjectData[property] === null) {
                    ignoreData = true
                    break
                }
            }
            if (ignoreData) continue
            if (!rel) {
                upsertRecords.push(upsertRecord)
                continue
            }

            const foreignInputRecordKeyOptions = inputPropertyKeyOptions
                .map((group) => group.map((k) => liveObjectData[k]).join(valueSep))
                .flat()

            let foreignInputRecordReferenceKeyValues = null
            for (const key of foreignInputRecordKeyOptions) {
                if (referenceKeyValues.hasOwnProperty(key)) {
                    foreignInputRecordReferenceKeyValues = referenceKeyValues[key] || []
                    break
                }
            }
            if (!foreignInputRecordReferenceKeyValues) continue

            for (const referenceKeyValue of foreignInputRecordReferenceKeyValues) {
                const record = { ...upsertRecord }
                const refKeyColValues = referenceKeyValue.split(valueSep)
                for (let i = 0; i < refKeyColValues.length; i++) {
                    record[rel.foreignKey[i]] = refKeyColValues[i]
                }
                upsertRecords.push(record)
            }
        }
        if (!upsertRecords.length) return

        const op = async () => {
            try {
                await db.transaction(async (tx) => {
                    this.seedCount += upsertRecords.length
                    logger.info(
                        chalk.cyanBright(
                            `  ${this.inputBatchSeedCount.toLocaleString('en-US')} records -> ${
                                this.seedTableName
                            }`
                        )
                    )
                    const upsertBatchOp = {
                        type: OpType.Insert,
                        schema: this.seedSchemaName,
                        table: this.seedTableName,
                        data: upsertRecords,
                        conflictTargets: this.enrichedLink.uniqueConstraint,
                        liveTableColumns: this.liveTableColumns,
                        primaryTimestampColumn: this.primaryTimestampColumn,
                        defaultColumnValues: this.defaultColumnValues,
                    }
                    await new RunOpService(upsertBatchOp, tx).perform()
                })
            } catch (err) {
                throw new QueryError('upsert', this.seedSchemaName, this.seedTableName, err)
            }
        }

        await withDeadlockProtection(op)
    }

    _transformRecordsIntoFunctionInputs(
        records: StringKeyMap[],
        primaryTablePath: string
    ): StringKeyMap {
        const typeConversionMap = {}
        let inputGroups = []

        for (let i = 0; i < this.requiredArgColPaths.length; i++) {
            const requiredArgColPaths = this.requiredArgColPaths[i]
            const colPathsToFunctionInputArgs = this.colPathsToFunctionInputArgs[i] || {}
            const valueFilters = this.valueFilters[i]
            const recordAgnosticFilters = this._formatValueFilterGroupAsInputArgs(valueFilters)
            const inputGroup = []

            for (const record of records) {
                const input = {}

                // Map each record to a function input payload.
                for (const colPath of requiredArgColPaths) {
                    const [colSchemaName, colTableName, colName] = colPath.split('.')
                    const colTablePath = [colSchemaName, colTableName].join('.')
                    const inputArgsWithOps = colPathsToFunctionInputArgs[colPath] || []

                    const recordColKey = colTablePath === primaryTablePath ? colName : colPath
                    if (!record.hasOwnProperty(recordColKey)) continue

                    for (const { property, op } of inputArgsWithOps) {
                        const originalValue = record[recordColKey]
                        let castedValue = originalValue

                        // Auto-lowercase addresses.
                        if (
                            originalValue &&
                            !!property.match(/address/i) &&
                            constants.MATCH_CASE_INSENSITIVE_ADDRESSES
                        ) {
                            castedValue = originalValue.toLowerCase()
                        }
                        // Auto-stringify chain ids.
                        else if (originalValue && !!property.match(/(chainId|chain_id|chainid)/i)) {
                            castedValue = originalValue.toString()
                        }
                        typeConversionMap[property] = typeConversionMap[property] || {}
                        typeConversionMap[property][castedValue] = originalValue
                        input[property] =
                            op === FilterOp.EqualTo ? castedValue : { op, value: castedValue }
                    }
                }

                inputGroup.push({
                    ...recordAgnosticFilters,
                    ...input,
                })
            }

            inputGroups.push(inputGroup)
        }

        inputGroups = inputGroups.map((group) => {
            const onlyHasOneProperty = Object.keys(group[0] || {}).length === 1
            if (onlyHasOneProperty) {
                return groupByKeys(group)
            }
            return group
        })

        return {
            batchFunctionInputs: inputGroups.length > 1 ? inputGroups.flat() : inputGroups[0],
            typeConversionMap,
        }
    }

    _formatValueFilterGroupAsInputArgs(valueFilters: { [key: string]: Filter }): StringKeyMap {
        const inputArgs = {}
        for (const key in valueFilters) {
            const filter = valueFilters[key]
            inputArgs[key] = filter.op === FilterOp.EqualTo ? filter.value : filter
        }
        return inputArgs
    }

    async _findInputRecordsFromAdjacentCols(
        queryConditions: StringKeyMap,
        offset: number,
        limit: number
    ): Promise<StringKeyMap[]> {
        // Start a new query on the table this live object is linked to.
        let query = db.from(this.seedTablePath)

        // Add JOIN conditions.
        for (let join of queryConditions.join) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, joinRefKey, joinForeignKey)
        }

        // Add SELECT conditions and order by primary keys.
        query.select(queryConditions.select).orderBy(this.seedTablePrimaryKeys.sort())

        // Add WHERE NOT NULL conditions.
        const whereNotNull = {}
        for (let i = 0; i < queryConditions.whereNotNull.length; i++) {
            whereNotNull[queryConditions.whereNotNull[i]] = null
        }
        query.whereNot(whereNotNull)

        // And are also not empty arrays.
        for (const arrayCol of queryConditions.arrayColumns) {
            query.where(arrayCol, '!=', '{}')
        }

        // Add offset/limit for batching.
        query.offset(offset).limit(limit)

        // Perform the query.
        try {
            return await query
        } catch (err) {
            throw new QueryError('select', this.seedSchemaName, this.seedTableName, err)
        }
    }

    _buildQueryForSeedWithAdjacentCols(): StringKeyMap {
        const queryConditions = {
            join: [],
            select: [`${this.seedTablePath}.*`],
            whereNotNull: [],
            arrayColumns: [],
        }

        for (const colPath of this.requiredArgColPaths) {
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = [colSchemaName, colTableName].join('.')

            if (colTablePath !== this.seedTablePath) {
                const rel = getRel(this.seedTablePath, colTablePath)
                if (!rel) throw `No rel from ${this.seedTablePath} -> ${colTablePath}`

                queryConditions.join.push([
                    colTableName,
                    `${colTablePath}.${rel.referenceKey}`,
                    `${this.seedTablePath}.${rel.foreignKey}`,
                ])

                queryConditions.select.push(`${colPath} as ${colPath}`)
                queryConditions.whereNotNull.push(colPath)
                if (isColTypeArray(colPath)) {
                    queryConditions.arrayColumns.push(colPath)
                }
            } else {
                queryConditions.whereNotNull.push(colName)
                if (isColTypeArray(colPath)) {
                    queryConditions.arrayColumns.push(colName)
                }
            }
        }

        return queryConditions
    }

    async _getForeignInputRecordsBatch(
        tablePath: string,
        primaryKeys: string[],
        tableInputColNames: string[],
        offset: number,
        limit: number
    ): Promise<StringKeyMap[] | null> {
        // Map col names to null (and separate out array column names).
        const whereNotNull = {}
        const arrayCols = []
        for (const colName of tableInputColNames) {
            whereNotNull[colName] = null
            if (isColTypeArray([tablePath, colName].join('.'))) {
                arrayCols.push(colName)
            }
        }

        // Find all in batch where input cols names are NOT null.
        const query = db
            .from(tablePath)
            .select('*')
            .orderBy(primaryKeys.sort())
            .whereNot(whereNotNull)

        // And are also not empty arrays.
        for (const arrayColName of arrayCols) {
            query.where(arrayColName, '!=', '{}')
        }

        try {
            return await query.offset(offset).limit(limit)
        } catch (err) {
            const [schema, table] = tablePath.split('.')
            logger.error(new QueryError('select', schema, table, err).message)
            return null
        }
    }

    async _updateOpTrackingFloor() {
        const lastUpdate = this.lastOpTrackingFloorUpdate
        const now = new Date()

        // @ts-ignore
        const timeSinceLastUpdate = now - lastUpdate
        if (
            lastUpdate &&
            timeSinceLastUpdate < constants.POLL_HEADS_DURING_LONG_RUNNING_SEEDS_INTERVAL
        ) {
            return
        }
        this.lastOpTrackingFloorUpdate = now

        const { data: mostRecentBlockNumbers, error } =
            await messageClient.getMostRecentBlockNumbers()
        if (error) {
            logger.error(`Not updating op-tracking floor. Got error: ${error}`)
            return
        }

        const opTrackingEntries = []
        for (const chainId of this.liveObjectChainIds) {
            if (!mostRecentBlockNumbers.hasOwnProperty(chainId)) continue
            const newOpTrackingFloor =
                Number(mostRecentBlockNumbers[chainId]) - constants.OP_TRACKING_FLOOR_OFFSET
            opTrackingEntries.push({
                tablePath: this.seedTablePath,
                chainId,
                isEnabledAbove: newOpTrackingFloor,
            })
        }

        opTrackingEntries.length && (await upsertOpTrackingEntries(opTrackingEntries))
    }

    _findRequiredArgColumns() {
        const requiredArgColPaths = []
        const colPathsToFunctionInputArgs = []
        const valueFilters = []

        for (const filterGroup of this.enrichedLink.filterBy) {
            const requiredArgColPathsEntry = new Set()
            const colPathsToFunctionInputArgsEntry = {}
            const valueFiltersEntry = {}

            for (const property in filterGroup) {
                const filter = filterGroup[property]

                if (filter.column) {
                    requiredArgColPathsEntry.add(filter.column)

                    if (!colPathsToFunctionInputArgsEntry.hasOwnProperty(filter.column)) {
                        colPathsToFunctionInputArgsEntry[filter.column] = []
                    }

                    if (filter.op !== FilterOp.EqualTo) {
                        // TODO
                    }

                    colPathsToFunctionInputArgsEntry[filter.column].push({
                        property,
                        op: filter.op,
                    })
                } else {
                    valueFiltersEntry[property] = filter
                }
            }

            requiredArgColPaths.push(Array.from(requiredArgColPathsEntry))
            colPathsToFunctionInputArgs.push(colPathsToFunctionInputArgsEntry)
            valueFilters.push(valueFiltersEntry)
        }

        this.requiredArgColPaths = requiredArgColPaths
        this.colPathsToFunctionInputArgs = colPathsToFunctionInputArgs
        this.valueFilters = valueFilters
    }

    _logResults(t0) {
        if (!t0) return
        const tf = performance.now()
        const seconds = Number(((tf - t0) / 1000).toFixed(2))
        logger.info(chalk.cyanBright('\nDone.'))

        if (this.seedCount === 0) {
            logger.info(chalk.cyanBright(`No ${this.seedTableName} records changed.`))
            return
        }

        logger.info(
            chalk.cyanBright(
                `Upserted ${this.seedCount.toLocaleString('en-US')} ${
                    this.seedTableName
                } records. in ${seconds} seconds.`
            )
        )
    }
}

export default SeedTableService
