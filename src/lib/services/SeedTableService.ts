import logger from '../logger'
import {
    SeedSpec,
    StringMap,
    LiveObject,
    EdgeFunction,
    LiveObjectFunctionRole,
    StringKeyMap,
    TableDataSources,
    OpType,
    ForeignKeyConstraint,
    Filter,
    FilterOp,
    ColumnDefaultsConfig,
} from '../types'
import { reverseMap, toMap, getCombinations, unique, groupByKeys } from '../utils/formatters'
import { areColumnsEmpty } from '../db/ops'
import RunOpService from './RunOpService'
import { callSpecFunction } from '../utils/requests'
import config from '../config'
import { db } from '../db'
import { QueryError } from '../errors'
import constants from '../constants'
import { tablesMeta, getRel, isColTypeArray } from '../db/tablesMeta'
import chalk from 'chalk'
import { updateCursor } from '../db/spec'
import LRU from 'lru-cache'
import { applyDefaults } from '../defaults'
import { withDeadlockProtection } from '../utils/db'

const valueSep = '__:__'

class SeedTableService {
    seedSpec: SeedSpec

    liveObject: LiveObject

    seedCursorId: string

    cursor: number

    seedFunction: EdgeFunction | null

    seedColNames: Set<string>

    requiredArgColPaths: string[] = []

    colPathsToFunctionInputArgs: StringKeyMap[] = []

    seedCount: number = 0

    inputBatchSeedCount: number = 0

    tableDataSources: TableDataSources

    seedTableUniqueConstraint: string[]

    filters: { [key: string]: Filter }[]

    liveTableColumns: string[]

    defaultColumnValues: { [key: string]: ColumnDefaultsConfig }

    seedWithColPaths: StringMap[]

    primaryTimestampColumn: string | null

    seedStrategy: () => void | null

    foreignKeyMappings: LRU<string, any> = new LRU({ max: 5000 })

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
        return toMap(this.seedSpec.linkProperties)
    }

    get reverseLinkProperties(): StringMap {
        return reverseMap(this.seedSpec.linkProperties)
    }

    get seedTablePrimaryKeys(): string[] {
        const meta = tablesMeta[this.seedTablePath]
        if (!meta) throw `No meta registered for table ${this.seedTablePath}`
        return meta.primaryKey.map((pk) => pk.name)
    }

    get primaryTimestampProperty(): string | null {
        return this.liveObject.config?.primaryTimestampProperty || null
    }

    constructor(seedSpec: SeedSpec, liveObject: LiveObject, seedCursorId: string, cursor: number) {
        this.seedSpec = seedSpec
        this.liveObject = liveObject
        this.seedCursorId = seedCursorId
        this.cursor = cursor || 0
        this.seedFunction = null
        this.seedColNames = new Set<string>(this.seedSpec.seedColNames)
        this.tableDataSources = config.getLiveObjectTableDataSources(
            this.liveObject.id,
            this.seedTablePath
        )
        this.seedTableUniqueConstraint = this._getUniqueConstraint()
        this.filters = this._buildFilters()
        this.defaultColumnValues = config.getDefaultColumnValuesForTable(this.seedTablePath)
        this.seedWithColPaths = config.getSeedColPaths(
            this.seedSpec.seedWith,
            this.seedSpec.linkProperties
        )
        this.liveTableColumns = Object.keys(
            config.getTable(this.seedSchemaName, this.seedTableName) || {}
        )
        this.primaryTimestampColumn = this.primaryTimestampProperty
            ? ((this.tableDataSources[this.primaryTimestampProperty] || [])[0]?.columnName || null)
            : null
        this.seedStrategy = null
    }

    async perform() {
        await this.determineSeedStrategy()
        await this.executeSeedStrategy()
    }

    async determineSeedStrategy() {
        // Find seed function to use.
        this._findSeedFunction()
        if (!this.seedFunction) throw "Live object doesn't have an associated seed function."

        // Find the required args for this function and their associated columns.
        this._findRequiredArgColumns()
        if (!this.requiredArgColPaths.length && !this.seedSpec.seedIfEmpty) {
            throw 'No required arg column paths found.'
        }

        const inputTableColumns = {}
        const inputColumnLocations = { onSeedTable: 0, onForeignTable: 0 }
        const uniqueTablePaths = new Set<string>()
        const uniqueRequiredArgColPaths = unique(this.requiredArgColPaths.flat())

        for (const colPath of uniqueRequiredArgColPaths) {
            const [schemaName, tableName, colName] = colPath.split('.')
            const tablePath = [schemaName, tableName].join('.')
            uniqueTablePaths.add(tablePath)

            tablePath === this.seedTablePath
                ? inputColumnLocations.onSeedTable++
                : inputColumnLocations.onForeignTable++

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
            if (inputColumnLocations.onForeignTable > 0) {
                logger.warn(
                    `${this.seedTablePath} - Can't seed a cross-table relationship from scratch.`
                )
                return
            }
            if (!this.seedSpec.seedIfEmpty) {
                logger.warn(
                    `${this.seedTablePath} - Table not configured to seed -- seedIfEmpty isn't truthy.`
                )
                return
            }
            this.seedStrategy = async () => await this._seedFromScratch()
        }
        // Seed using the seed table as the target of the query.
        else if (inputColumnLocations.onSeedTable > 0) {
            this.seedStrategy = async () => await this._seedWithAdjacentCols()
        }
        // TODO: This is possible, just gonna take a lot more lookup work.
        else if (inputColumnLocations.onForeignTable > 1 && uniqueTablePaths.size > 1) {
            logger.warn(
                `${this.seedTablePath} - Can't yet seed tables using exclusively more than one foreign table.`
            )
        }
        // Seed using the foreign table as the target of the query.
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
        // Find seed function to use.
        this._findSeedFunction()
        if (!this.seedFunction) throw "Live object doesn't have an associated seed function."

        // Find the required args for this function and their associated columns.
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

        // TODO: Implement filters here too.
        const [staticFilters, _] = this.filters

        const sharedErrorContext = { error: null }
        const t0 = performance.now()
        try {
            await callSpecFunction(
                this.seedFunction,
                {},
                async (data) =>
                    this._handleDataOnSeedFromScratch(data as StringKeyMap[]).catch((err) => {
                        sharedErrorContext.error = err
                    }),
                sharedErrorContext
            )
        } catch (err) {
            logger.error(err)
            throw err
        }

        const tf = performance.now()
        const seconds = Number(((tf - t0) / 1000).toFixed(2))
        const rate = Math.round(this.seedCount / seconds)
        logger.info(chalk.cyanBright('Done.'))
        logger.info(
            chalk.cyanBright(
                `Upserted ${this.seedCount.toLocaleString(
                    'en-US'
                )} records in ${seconds} seconds (${rate.toLocaleString('en-US')} rows/s)`
            )
        )
    }

    async _seedWithAdjacentCols() {
        logger.info(chalk.cyanBright(`\nSeeding ${this.seedTablePath} from adjacent columns...`))

        const queryConditions = this._buildQueryForSeedWithAdjacentCols()

        // TODO: Actually implement potential column filters here.
        const [staticFilters, _] = this.filters

        // Get the live object property keys associated with each input column.
        const reverseLinkProperties = this.reverseLinkProperties
        const inputPropertyKeys = this.requiredArgColPaths.map(
            (colPath) => reverseLinkProperties[colPath]
        )

        // If any of the input columns is of array type, shrink the seed input batch size to 1.
        const seedInputBatchSize = queryConditions.arrayColumns.length
            ? 1
            : constants.SEED_INPUT_BATCH_SIZE

        // Start seeding with batches of input records.
        const sharedErrorContext = { error: null }
        let t0 = null
        while (true) {
            if (sharedErrorContext.error) throw sharedErrorContext.error

            // Get batch of input records from the seed table.
            const batchInputRecords = await this._findInputRecordsFromAdjacentCols(
                queryConditions,
                this.cursor,
                seedInputBatchSize
            )
            if (batchInputRecords === null) {
                throw `Foreign input records batch came up null for ${this.seedTablePath} at offset ${this.cursor}.`
            }

            this.cursor += batchInputRecords.length
            const isLastBatch = batchInputRecords.length < seedInputBatchSize

            const batchFunctionInputs = []
            const indexedPkConditions = {}
            for (const record of batchInputRecords) {
                const input = {}
                const colValues = []

                for (const colPath of this.requiredArgColPaths) {
                    const [colSchemaName, colTableName, colName] = colPath.split('.')
                    const colTablePath = [colSchemaName, colTableName].join('.')
                    const inputArg = this.colPathsToFunctionInputArgs[colPath]
                    const recordColKey = colTablePath === this.seedTablePath ? colName : colPath
                    const value = record[recordColKey]
                    input[inputArg] = value
                    colValues.push(value)
                }

                batchFunctionInputs.push(input)

                const recordPrimaryKeys = {}
                for (const pk of this.seedTablePrimaryKeys) {
                    recordPrimaryKeys[pk] = record[pk]
                }

                const colValueOptions = getCombinations(colValues)
                for (const valueOptions of colValueOptions) {
                    const key = valueOptions.join(valueSep)
                    indexedPkConditions[key] = indexedPkConditions[key] || []
                    indexedPkConditions[key].push(recordPrimaryKeys)
                }
            }
            const batchFunctionInput = groupByKeys(batchFunctionInputs)

            // Callback to use when a batch of response data is available.
            const onFunctionRespData = async (data) =>
                this._handleDataOnAdjacentColsSeed(
                    data,
                    inputPropertyKeys,
                    indexedPkConditions
                ).catch((err) => {
                    sharedErrorContext.error = err
                })

            // Call spec function and handle response data.
            t0 = t0 || performance.now()
            try {
                await callSpecFunction(
                    this.seedFunction,
                    batchFunctionInput,
                    onFunctionRespData,
                    sharedErrorContext
                )
            } catch (err) {
                logger.error(err)
                throw err
            }

            if (sharedErrorContext.error) throw sharedErrorContext.error

            await updateCursor(this.seedCursorId, this.cursor)

            if (isLastBatch) {
                this._logResults(t0)
                break
            }
        }
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
        const inputPropertyKeys = inputColNames.map(
            (colName) => reverseLinkProperties[`${foreignTablePath}.${colName}`]
        )
        const arrayInputColNames = inputColNames.filter((colName) =>
            isColTypeArray([foreignTablePath, colName].join('.'))
        )

        // Get link properties / reference keys associated with any *OTHER* linked foreign tables.
        const linkProperties = this.linkProperties
        const nonSeedLinkedForeignTableData = []
        const seen = new Set()

        for (const property in linkProperties) {
            const colPath = linkProperties[property]
            const [schema, table, colName] = colPath.split('.')
            const tablePath = [schema, table].join('.')

            if (seen.has(tablePath)) continue
            seen.add(tablePath)

            if (tablePath !== this.seedTablePath && tablePath !== foreignTablePath) {
                const rel = getRel(this.seedTablePath, tablePath)
                nonSeedLinkedForeignTableData.push({
                    tablePath,
                    property,
                    colName,
                    foreignKey: rel?.foreignKey,
                    referenceKey: rel?.referenceKey,
                })
            }
        }

        const [staticFilters, columnFilters] = this.filters
        const hasColumnFilters = Object.keys(columnFilters).length > 0

        // If any of the input columns is of array type, OR, column filters are being used,
        // shrink the seed input batch size to 1.
        const seedInputBatchSize =
            arrayInputColNames.length || hasColumnFilters
                ? 1
                : constants.FOREIGN_SEED_INPUT_BATCH_SIZE

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
            const isLastBatch = batchInputRecords.length < seedInputBatchSize

            // Map the input records to their reference key values so that records being added
            // to the seed table (later) can easily find/assign their foreign keys.
            const referenceKeyValues = {}
            if (rel) {
                for (const record of batchInputRecords) {
                    const colValues = inputColNames.map((colName) => record[colName])
                    const colValueOptions = getCombinations(colValues)

                    for (const valueOptions of colValueOptions) {
                        const key = valueOptions.join(valueSep)
                        referenceKeyValues[key] = referenceKeyValues[key] || []
                        referenceKeyValues[key].push(record[rel.referenceKey])
                    }
                }
            }

            // Transform the records into seed-function inputs.
            const batchFunctionInputs = this._transformRecordsIntoFunctionInputs(
                batchInputRecords,
                foreignTablePath,
                staticFilters,
                columnFilters
            )

            // Callback to use when a batch of response data is available.
            const onFunctionRespData = async (data) =>
                this._handleDataOnForeignTableSeed(
                    data,
                    rel,
                    inputPropertyKeys,
                    referenceKeyValues,
                    nonSeedLinkedForeignTableData
                ).catch((err) => {
                    sharedErrorContext.error = err
                })

            // Call spec function and handle response data.
            this.inputBatchSeedCount = 0
            t0 = t0 || performance.now()
            try {
                await callSpecFunction(
                    this.seedFunction,
                    batchFunctionInputs,
                    onFunctionRespData,
                    sharedErrorContext
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

            if (isLastBatch) {
                this._logResults(t0)
                break
            }
        }
    }

    async _handleDataOnSeedFromScratch(batch: StringKeyMap[]) {
        this.seedCount += batch.length
        logger.info(
            chalk.cyanBright(
                `  ${this.seedCount.toLocaleString('en-US')} records -> ${this.seedTableName}`
            )
        )

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
            conflictTargets: this.seedTableUniqueConstraint,
            liveTableColumns: this.liveTableColumns,
            primaryTimestampColumn: this.primaryTimestampColumn,
            defaultColumnValues: this.defaultColumnValues,
        }

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
        inputPropertyKeys: string[],
        referenceKeyValues: StringKeyMap,
        nonSeedLinkedForeignTableData: StringKeyMap[]
    ) {
        this.inputBatchSeedCount += batch.length
        this.seedCount += batch.length
        logger.info(
            chalk.cyanBright(
                `  ${this.inputBatchSeedCount.toLocaleString('en-US')} records -> ${
                    this.seedTableName
                }`
            )
        )

        // Clone this so we can add to it with each batch independently.
        const otherLinkedForeignTables: StringKeyMap[] = [...nonSeedLinkedForeignTableData].map(
            (v) => ({ ...v, colValues: [] })
        )

        const linkProperties = this.linkProperties
        const tableDataSources = this.tableDataSources
        const upsertRecords = []

        for (const liveObjectData of batch) {
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

            const otherForeignLookups = {}
            for (let i = 0; i < otherLinkedForeignTables.length; i++) {
                const otherLinkedForeignTableEntry = otherLinkedForeignTables[i]
                const { tablePath, colName, property, foreignKey } = otherLinkedForeignTableEntry
                if (!foreignKey) continue
                const foreignColValue = liveObjectData[property]
                const foreignMappingKey = [tablePath, colName, property, foreignColValue].join('.')

                if (this.foreignKeyMappings.has(foreignMappingKey)) {
                    upsertRecord[foreignKey] = this.foreignKeyMappings.get(foreignMappingKey)
                } else {
                    otherLinkedForeignTables[i].colValues.push(foreignColValue)
                    otherForeignLookups[tablePath] = foreignColValue
                }
            }
            if (Object.keys(otherForeignLookups).length > 0) {
                upsertRecord._otherForeignLookups = otherForeignLookups
            }
            if (!rel) {
                upsertRecords.push(upsertRecord)
                continue
            }

            const foreignInputRecordKey = inputPropertyKeys
                .map((k) => liveObjectData[k])
                .join(valueSep)
            if (!referenceKeyValues.hasOwnProperty(foreignInputRecordKey)) continue

            const foreignInputRecordReferenceKeyValues =
                referenceKeyValues[foreignInputRecordKey] || []
            for (const referenceKeyValue of foreignInputRecordReferenceKeyValues) {
                const record = { ...upsertRecord }
                record[rel.foreignKey] = referenceKeyValue
                upsertRecords.push(record)
            }
        }
        if (!upsertRecords.length) return

        const op = async () => {
            try {
                await db.transaction(async (tx) => {
                    const resolvedDependentForeignTables = await Promise.all(
                        otherLinkedForeignTables
                            .filter((v) => v.colValues.length > 0)
                            .map((v) => this._findOrCreateDependentForeignTable(v, tx))
                    )
                    const resolvedDependentForeignTablesMap = {}
                    for (const resolvedDependentForeignTable of resolvedDependentForeignTables) {
                        resolvedDependentForeignTablesMap[resolvedDependentForeignTable.tablePath] =
                            resolvedDependentForeignTable
                    }
    
                    const finalUpsertRecords = []
                    for (const upsertRecord of upsertRecords) {
                        if (!upsertRecord.hasOwnProperty('_otherForeignLookups')) {
                            finalUpsertRecords.push(upsertRecord)
                            continue
                        }
    
                        let ignoreRecord = false
                        for (const foreignTablePath in upsertRecord._otherForeignLookups) {
                            const colValueUsedAtLookup =
                                upsertRecord._otherForeignLookups[foreignTablePath]
                            const resolvedDependentForeignTable =
                                resolvedDependentForeignTablesMap[foreignTablePath] || {}
                            const referenceKeyValuesMap =
                                resolvedDependentForeignTable.referenceKeyValuesMap || {}
                            const foreignKey = resolvedDependentForeignTable.foreignKey
    
                            if (!referenceKeyValuesMap.hasOwnProperty(colValueUsedAtLookup)) {
                                ignoreRecord = true
                                break
                            }
    
                            upsertRecord[foreignKey] = referenceKeyValuesMap[colValueUsedAtLookup]
                        }
                        if (ignoreRecord) continue
    
                        delete upsertRecord._otherForeignLookups
                        finalUpsertRecords.push(upsertRecord)
                    }
                    if (!finalUpsertRecords.length) return
    
                    const upsertBatchOp = {
                        type: OpType.Insert,
                        schema: this.seedSchemaName,
                        table: this.seedTableName,
                        data: finalUpsertRecords,
                        conflictTargets: this.seedTableUniqueConstraint,
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

    async _findOrCreateDependentForeignTable(otherLinkedForeignTableEntry, tx) {
        const { tablePath, property, colName, foreignKey, referenceKey } =
            otherLinkedForeignTableEntry
        const colValues = unique(otherLinkedForeignTableEntry.colValues)
        const resp = {
            tablePath,
            property,
            colName,
            foreignKey,
            referenceKey,
            referenceKeyValuesMap: {},
        }
        if (!colValues.length || !foreignKey || !referenceKey) return resp

        const colValuesSet = new Set(colValues)

        let data = colValues.map((colValue) => ({ [colName]: colValue }))

        // Apply any default column values configured by the user.
        const defaultColValues = config.getDefaultColumnValuesForTable(tablePath)
        if (Object.keys(defaultColValues).length) {
            data = applyDefaults(data, defaultColValues) as StringKeyMap[]
        }

        // TODO: If colName doesn't have a unique constraint, this method will fail
        // and you'll need to do a classic find...create.
        let results = []
        try {
            results = await tx(tablePath)
                .returning(unique([referenceKey, colName]))
                .insert(data)
                .onConflict(colName)
                .ignore()

            results = results || []

            if (results.length < data.length) {
                for (const newRecord of results) {
                    colValuesSet.delete(newRecord[colName])
                }
                const existingRecordColValues = Array.from(colValuesSet)
                if (existingRecordColValues.length) {
                    const existingResults = await tx(tablePath)
                        .select(unique([referenceKey, colName]))
                        .whereIn(colName, existingRecordColValues)
                    results.push(...(existingResults || []))
                }
            }
        } catch (err) {
            logger.error(`Error creating dependent foreign table ${tablePath}: ${err}`)
            return resp
        }

        const referenceKeyValuesMap = {}
        for (const result of results || []) {
            const colValue = result[colName]
            const referenceKeyValue = result[referenceKey]
            referenceKeyValuesMap[colValue] = referenceKeyValue

            // TODO: Only bring back once you've debugged issue with duplicate seed col values.
            // this.foreignKeyMappings.set([tablePath, colName, property, colValue].join('.'), referenceKeyValue)
        }

        resp.referenceKeyValuesMap = referenceKeyValuesMap
        return resp
    }

    async _handleDataOnAdjacentColsSeed(
        batch: StringKeyMap[],
        inputPropertyKeys: string[],
        indexedPkConditions: StringKeyMap
    ) {
        this.seedCount += batch.length
        logger.info(
            chalk.cyanBright(
                `  ${this.seedCount.toLocaleString('en-US')} records -> ${this.seedTableName}`
            )
        )

        const tableDataSources = this.tableDataSources
        const linkProperties = this.linkProperties
        const updates: StringKeyMap = {}

        for (const liveObjectData of batch) {
            // Format a seed table record for this live object data.
            const recordUpdates = {}
            for (const property in liveObjectData) {
                if (linkProperties.hasOwnProperty(property)) continue
                const colsWithThisPropertyAsDataSource = tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    if (this.seedColNames.has(columnName)) {
                        recordUpdates[columnName] = value
                    }
                }
            }
            if (!Object.keys(recordUpdates).length) continue

            // Find the primary key groups to apply the updates to.
            const pkConditionsKey = inputPropertyKeys.map((k) => liveObjectData[k]).join(valueSep)
            const primaryKeyConditions = indexedPkConditions[pkConditionsKey] || []
            if (!primaryKeyConditions?.length) continue

            // Merge updates by the actual primary key values.
            for (const pkConditions of primaryKeyConditions) {
                const uniquePkKey = Object.keys(pkConditions)
                    .sort()
                    .map((k) => pkConditions[k])
                    .join(valueSep)
                updates[uniquePkKey] = updates[uniquePkKey] || {
                    where: pkConditions,
                    updates: {},
                }
                updates[uniquePkKey].updates = { ...updates[uniquePkKey].updates, ...recordUpdates }
            }
        }
        if (!Object.keys(updates)) return

        const useBulkUpdate = batch.length > constants.MAX_UPDATES_BEFORE_BULK_UPDATE_USED
        const bulkWhere = []
        const bulkUpdates = []
        const indivUpdateOps = []
        for (const entry of Object.values(updates)) {
            if (useBulkUpdate) {
                bulkWhere.push(entry.where)
                bulkUpdates.push(entry.updates)
            } else {
                indivUpdateOps.push({
                    type: OpType.Update,
                    schema: this.seedSchemaName,
                    table: this.seedTableName,
                    where: entry.where,
                    data: entry.updates,
                    liveTableColumns: this.liveTableColumns,
                    primaryTimestampColumn: this.primaryTimestampColumn,
                    defaultColumnValues: this.defaultColumnValues,
                })
            }
        }

        const op = async () => {
            try {
                if (useBulkUpdate) {
                    const op = {
                        type: OpType.Update,
                        schema: this.seedSchemaName,
                        table: this.seedTableName,
                        where: bulkWhere,
                        data: bulkUpdates,
                        liveTableColumns: this.liveTableColumns,
                        primaryTimestampColumn: this.primaryTimestampColumn,
                        defaultColumnValues: this.defaultColumnValues,
                    }
                    await new RunOpService(op).perform()
                } else {
                    await db.transaction(async (tx) => {
                        await Promise.all(
                            indivUpdateOps.map((op) => new RunOpService(op, tx).perform())
                        )
                    })
                }
            } catch (err) {
                throw new QueryError('update', this.seedSchemaName, this.seedTableName, err)
            }
        }

        await withDeadlockProtection(op)
    }

    _transformRecordsIntoFunctionInputs(
        records: StringKeyMap[],
        primaryTablePath: string,
        staticFilters: { [key: string]: Filter },
        columnFilters: { [key: string]: Filter }
    ): StringKeyMap | StringKeyMap[] {
        // Build record-agnostic filters.
        const recordAgnosticFilters = {}
        for (const key in staticFilters) {
            const filter = staticFilters[key]
            if (filter.op === FilterOp.EqualTo) {
                recordAgnosticFilters[key] = filter.value
            } else {
                recordAgnosticFilters[key] = filter
            }
        }

        let inputs = []
        for (let i = 0; i < this.requiredArgColPaths.length; i++) {
            const requiredArgColPaths = this.requiredArgColPaths[i]
            const colPathsToFunctionInputArgs = this.colPathsToFunctionInputArgs[i]
            const input = []

            for (const record of records) {
                const entry = {}

                // Map each record to a function input payload.
                for (const colPath of requiredArgColPaths) {
                    const [colSchemaName, colTableName, colName] = colPath.split('.')
                    const colTablePath = [colSchemaName, colTableName].join('.')
                    const inputArgs = colPathsToFunctionInputArgs[colPath] || []

                    for (const inputArg of inputArgs) {
                        const recordColKey = colTablePath === primaryTablePath ? colName : colPath
                        entry[inputArg] = record[recordColKey]
                    }
                }

                // Build record-specific filters.
                // NOTE: If any record-specific/column filters exist, the batch size is forced
                // down to 1, so `records` is actually only has a length of 1.
                const recordSpecificFilters = {}
                for (const key in columnFilters) {
                    const filter = columnFilters[key]
                    const filterColPath = filter.column
                    const [schema, table, filterColName] = filterColPath.split('.')
                    const filterColTablePath = [schema, table].join('.')
                    const filterRecordColKey =
                        filterColTablePath === primaryTablePath ? filterColName : filterColPath
                    if (!record.hasOwnProperty(filterRecordColKey)) continue
                    const filterValue = record[filterRecordColKey]
                    if (filterValue === null) continue

                    if (filter.op === FilterOp.EqualTo) {
                        recordSpecificFilters[key] = filterValue
                    } else {
                        recordSpecificFilters[key] = {
                            op: filter.op,
                            value: filterValue,
                        }
                    }
                }

                input.push({
                    ...recordAgnosticFilters,
                    ...recordSpecificFilters,
                    ...entry,
                })
            }

            inputs.push(input)
        }

        inputs = inputs.map(groupByKeys)

        return inputs.length > 1 ? inputs : inputs[0]
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

    _findSeedFunction() {
        for (const edgeFunction of this.liveObject.edgeFunctions) {
            // HACK: For now just take the first GetMany function.
            if (edgeFunction.role === LiveObjectFunctionRole.GetMany) {
                this.seedFunction = edgeFunction
                break
            }
        }
    }

    _findRequiredArgColumns() {
        const requiredArgColPaths = []
        const colPathsToFunctionInputArgs = []

        for (const seedWithEntry of this.seedWithColPaths) {
            const requiredArgColPathsEntry = []
            const colPathsToFunctionInputArgsEntry = {}

            for (const property in seedWithEntry) {
                const colPath = seedWithEntry[property]

                if (!requiredArgColPathsEntry.includes(colPath)) {
                    requiredArgColPathsEntry.push(colPath)
                }

                if (!colPathsToFunctionInputArgsEntry.hasOwnProperty(colPath)) {
                    colPathsToFunctionInputArgsEntry[colPath] = []
                }
                colPathsToFunctionInputArgsEntry[colPath].push(property)
            }
            if (!requiredArgColPathsEntry.length) continue

            requiredArgColPaths.push(requiredArgColPathsEntry)
            colPathsToFunctionInputArgs.push(colPathsToFunctionInputArgsEntry)
        }

        this.requiredArgColPaths = requiredArgColPaths
        this.colPathsToFunctionInputArgs = colPathsToFunctionInputArgs
    }

    _buildFilters(): { [key: string]: Filter }[] {
        const objectFilters = this.liveObject.filterBy || {}
        const linkFilters = this.seedSpec.filterBy || {}
        const filters = { ...objectFilters, ...linkFilters }
        if (!Object.keys(filters).length) return [{}, {}]
        return config.categorizeFilters(filters)
    }

    _getUniqueConstraint(): string[] {
        const uniqueConstraint = config.getUniqueConstraintForLink(
            this.liveObject.id,
            this.seedTablePath
        )
        if (!uniqueConstraint) {
            throw `No unique constraint for link ${this.liveObject.id} <-> ${this.seedTablePath}`
        }
        return uniqueConstraint
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
                } records in ${seconds} seconds.`
            )
        )
    }
}

export default SeedTableService
