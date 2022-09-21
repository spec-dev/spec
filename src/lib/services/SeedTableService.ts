import logger from '../logger'
import {
    SeedSpec,
    StringMap,
    LiveObject,
    EdgeFunction,
    LiveObjectFunctionRole,
    StringKeyMap,
    TableDataSources,
    Op,
    OpType,
    ForeignKeyConstraint,
} from '../types'
import { reverseMap, toMap, getCombinations, unique } from '../utils/formatters'
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

const valueSep = '__:__'

class SeedTableService {
    seedSpec: SeedSpec

    liveObject: LiveObject

    seedCursorId: string

    cursor: number

    seedFunction: EdgeFunction | null

    seedColNames: Set<string>

    requiredArgColPaths: string[] = []

    colPathToFunctionInputArg: { [key: string]: string } = {}

    seedCount: number = 0

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

    get tableDataSources(): TableDataSources {
        return config.getLiveObjectTableDataSources(this.liveObject.id, this.seedTablePath)
    }

    get seedTablePrimaryKeys(): string[] {
        const meta = tablesMeta[this.seedTablePath]
        if (!meta) throw `No meta registered for table ${this.seedTablePath}`
        return meta.primaryKey.map((pk) => pk.name)
    }

    get seedTableUniqueConstraint(): string[] {
        const uniqueConstaint = config.getUniqueConstraintForLink(
            this.liveObject.id,
            this.seedTablePath
        )
        if (!uniqueConstaint)
            throw `No unique constraint for link ${this.liveObject.id} <-> ${this.seedTablePath}`
        return uniqueConstaint
    }

    get defaultFilters(): StringKeyMap {
        return this.liveObject.filterBy || {}
    }

    constructor(seedSpec: SeedSpec, liveObject: LiveObject, seedCursorId: string, cursor: number) {
        this.seedSpec = seedSpec
        this.liveObject = liveObject
        this.seedCursorId = seedCursorId
        this.cursor = cursor || 0
        this.seedFunction = null
        this.seedColNames = new Set<string>(this.seedSpec.seedColNames)
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
        if (!this.requiredArgColPaths.length) throw 'No required arg column paths found.'

        // TODO: Break out.
        const inputTableColumns = {}
        const inputColumnLocations = { onSeedTable: 0, onForeignTable: 0 }
        const uniqueTablePaths = new Set<string>()

        for (const colPath of this.requiredArgColPaths) {
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

        // TODO: Break out.
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
                `${this.seedTablePath} - Can't seed table using exclusively more than one foreign table.`
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
        for (const colPath of this.requiredArgColPaths) {
            const [schemaName, tableName, colName] = colPath.split('.')
            const tablePath = [schemaName, tableName].join('.')

            if (tablePath !== foreignTablePath) {
                const err = `seedWithForeignRecords can only be used if all 
                required arg cols exist on the given foreign table: ${foreignTablePath}; 
                requiredArgColPaths: ${this.requiredArgColPaths}.`
                throw err
            }

            inputColNames.push(colName)
        }

        // Seed with the explicitly given foreign input records.
        await this._seedWithForeignTable(foreignTablePath, inputColNames, records)
    }

    async _seedFromScratch() {
        logger.info(chalk.cyanBright(`Seeding ${this.seedTablePath} from scratch...`))

        const t0 = performance.now()
        try {
            await callSpecFunction(this.seedFunction, this.defaultFilters, async (data) => {
                await this._handleDataOnSeedFromScratch(data as StringKeyMap[])
            })
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
        logger.info(chalk.cyanBright(`Seeding ${this.seedTablePath} from adjacent columns...`))

        const queryConditions = this._buildQueryForSeedWithAdjacentCols()

        // Get the live object property keys associated with each input column.
        const reverseLinkProperties = this.reverseLinkProperties
        const inputPropertyKeys = this.requiredArgColPaths.map(
            (colPath) => reverseLinkProperties[colPath]
        )

        // If any of the input columns is of array type, shrink the seed input batch size to 1.
        const seedInputBatchSize = queryConditions.hasArrayColumns ? 1 : constants.SEED_INPUT_BATCH_SIZE

        // Start seeding with batches of input records.
        let t0 = null
        while (true) {
            // Get batch of input records from the seed table.
            const batchInputRecords = await this._findInputRecordsFromAdjacentCols(
                queryConditions,
                this.cursor,
                seedInputBatchSize,
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
                    const inputArg = this.colPathToFunctionInputArg[colPath]
                    const recordColKey = colTablePath === this.seedTablePath ? colName : colPath
                    const value = record[recordColKey]
                    input[inputArg] = value
                    colValues.push(value)
                }

                batchFunctionInputs.push({ ...this.defaultFilters, ...input })

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

            // Callback to use when a batch of response data is available.
            const onFunctionRespData = async (data) =>
                await this._handleDataOnAdjacentColsSeed(
                    data,
                    inputPropertyKeys,
                    indexedPkConditions
                )

            // Call spec function and handle response data.
            t0 = t0 || performance.now()
            try {
                await callSpecFunction(this.seedFunction, batchFunctionInputs, onFunctionRespData)
            } catch (err) {
                logger.error(err)
                throw err
            }

            await updateCursor(this.seedCursorId, this.cursor)

            if (isLastBatch) {
                const tf = performance.now()
                const seconds = Number(((tf - t0) / 1000).toFixed(2))
                const rate = Math.round(this.seedCount / seconds)
                logger.info(chalk.cyanBright('Done.'))
                logger.info(
                    chalk.cyanBright(
                        `Updated ${this.seedCount.toLocaleString(
                            'en-US'
                        )} records in ${seconds} seconds (${rate.toLocaleString('en-US')} rows/s)`
                    )
                )
                break
            }
        }
    }

    async _seedWithForeignTable(
        foreignTablePath: string,
        inputColNames: string[],
        inputRecords?: StringKeyMap[]
    ) {
        logger.info(
            chalk.cyanBright(
                `Seeding ${this.seedTablePath} with foreign table ${foreignTablePath}...`
            )
        )

        // Get seed table -> foreign table relationship.
        const rel = getRel(this.seedTablePath, foreignTablePath)
        if (!rel) throw `No relationship ${this.seedTablePath} -> ${foreignTablePath} exists.`
        const foreignTablePrimaryKeys = tablesMeta[foreignTablePath].primaryKey.map((pk) => pk.name)

        // Get the live object property keys associated with each input column.
        const reverseLinkProperties = this.reverseLinkProperties
        const inputPropertyKeys = inputColNames.map(
            (colName) => reverseLinkProperties[`${foreignTablePath}.${colName}`]
        )
        const arrayInputColNames = inputColNames
            .filter(colName => isColTypeArray([foreignTablePath, colName].join('.')))

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
                if (!rel) throw `No relationship ${this.seedTablePath} -> ${tablePath} exists.`

                nonSeedLinkedForeignTableData.push({
                    tablePath,
                    property,
                    colName,
                    foreignKey: rel.foreignKey,
                    referenceKey: rel.referenceKey,
                })
            }
        }

        // If any of the input columns is of array type, shrink the seed input batch size to 1.
        const seedInputBatchSize = arrayInputColNames.length ? 1 : constants.FOREIGN_SEED_INPUT_BATCH_SIZE

        // Start seeding with batches of input records.
        let t0 = null
        while (true) {
            logger.info(chalk.cyanBright('\nNEW INPUT BATCH\n'))

            // Get batch of input records from the foreign table.
            const batchInputRecords =
                inputRecords ||
                (await this._getForeignInputRecordsBatch(
                    foreignTablePath,
                    foreignTablePrimaryKeys as string[],
                    inputColNames,
                    this.cursor,
                    seedInputBatchSize,
                ))
            if (batchInputRecords === null) {
                throw `Foreign input records batch came up null for ${foreignTablePath} at offset ${this.cursor}.`
            }

            this.cursor += batchInputRecords.length
            const isLastBatch = !!inputRecords || batchInputRecords.length < seedInputBatchSize

            // Map the input records to their reference key values so that records being added
            // to the seed table (later) can easily find/assign their foreign keys.
            const referenceKeyValues = {}
            for (const record of batchInputRecords) {
                const colValues = inputColNames.map((colName) => record[colName])
                const colValueOptions = getCombinations(colValues)

                for (const valueOptions of colValueOptions) {
                    const key = valueOptions.join(valueSep)
                    referenceKeyValues[key] = referenceKeyValues[key] || []
                    referenceKeyValues[key].push(record[rel.referenceKey])    
                }
            }

            // Transform the records into seed-function inputs.
            const batchFunctionInputs = this._transformRecordsIntoFunctionInputs(
                batchInputRecords,
                foreignTablePath,
            )

            // Callback to use when a batch of response data is available.
            const onFunctionRespData = async (data) =>
                this._handleDataOnForeignTableSeed(
                    data,
                    rel,
                    inputPropertyKeys,
                    referenceKeyValues,
                    nonSeedLinkedForeignTableData,
                )

            // Call spec function and handle response data.
            t0 = t0 || performance.now()
            try {
                await callSpecFunction(this.seedFunction, batchFunctionInputs, onFunctionRespData)
            } catch (err) {
                logger.error(err)
                throw err
            }

            await updateCursor(this.seedCursorId, this.cursor)

            if (isLastBatch) {
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
                break
            }
        }
    }

    async _handleDataOnSeedFromScratch(batch: StringKeyMap[]) {
        this.seedCount += batch.length
        logger.info(chalk.cyanBright(`  ${this.seedCount.toLocaleString('en-US')}`))

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
        }

        try {
            await db.transaction(async (tx) => {
                await new RunOpService(insertBatchOp, tx).perform()
            })
        } catch (err) {
            throw new QueryError('insert', this.seedSchemaName, this.seedTableName, err)
        }
    }

    async _handleDataOnForeignTableSeed(
        batch: StringKeyMap[],
        rel: ForeignKeyConstraint,
        inputPropertyKeys: string[],
        referenceKeyValues: StringKeyMap,
        nonSeedLinkedForeignTableData: StringKeyMap[],
    ) {
        this.seedCount += batch.length
        logger.info(chalk.cyanBright(`  ${this.seedCount.toLocaleString('en-US')}`))

        // Clone this so we can add to it with each batch independently.
        const otherLinkedForeignTables: StringKeyMap[] = [
            ...nonSeedLinkedForeignTableData
        ].map(v => ({ ...v, colValues: [] }))

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

            const foreignInputRecordKey = inputPropertyKeys.map(k => liveObjectData[k]).join(valueSep)
            if (!referenceKeyValues.hasOwnProperty(foreignInputRecordKey)) continue

            const otherForeignLookups = {}
            for (let i = 0; i < otherLinkedForeignTables.length; i++) {
                const otherLinkedForeignTableEntry = otherLinkedForeignTables[i]
                const { tablePath, colName, property, foreignKey } = otherLinkedForeignTableEntry
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

            const foreignInputRecordReferenceKeyValues = referenceKeyValues[foreignInputRecordKey] || []
            for (const referenceKeyValue of foreignInputRecordReferenceKeyValues) {
                const record = { ...upsertRecord }
                record[rel.foreignKey] = referenceKeyValue
                upsertRecords.push(record)
            }
        }
        if (!upsertRecords.length) return

        try {
            await db.transaction(async (tx) => {
                const resolvedDependentForeignTables = await Promise.all(
                    otherLinkedForeignTables.map(v => this._findOrCreateDependentForeignTable(v, tx))
                )
                const resolvedDependentForeignTablesMap = {}
                for (const resolvedDependentForeignTable of resolvedDependentForeignTables) {
                    resolvedDependentForeignTablesMap[resolvedDependentForeignTable.tablePath] = resolvedDependentForeignTable
                }

                const finalUpsertRecords = []
                for (const upsertRecord of upsertRecords) {
                    if (!upsertRecord.hasOwnProperty('_otherForeignLookups')) {
                        finalUpsertRecords.push(upsertRecord)
                        continue
                    }

                    let ignoreRecord = false
                    for (const foreignTablePath in upsertRecord._otherForeignLookups) {
                        const colValueUsedAtLookup = upsertRecord._otherForeignLookups[foreignTablePath]
                        const resolvedDependentForeignTable = resolvedDependentForeignTablesMap[foreignTablePath] || {}
                        const { referenceKeyValuesMap = {}, foreignKey } = resolvedDependentForeignTable

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
                
                const upsertBatchOp = {
                    type: OpType.Insert,
                    schema: this.seedSchemaName,
                    table: this.seedTableName,
                    data: upsertRecords,
                    conflictTargets: this.seedTableUniqueConstraint,
                }
        
                await new RunOpService(upsertBatchOp, tx).perform()
            })
        } catch (err) {
            throw new QueryError('upsert', this.seedSchemaName, this.seedTableName, err)
        }
    }

    async _findOrCreateDependentForeignTable(otherLinkedForeignTableEntry, tx) {
        const {
            tablePath,
            property,
            colName,
            foreignKey,
            referenceKey,
        } = otherLinkedForeignTableEntry
        const colValues = unique(otherLinkedForeignTableEntry.colValues)

        const resp = {
            tablePath,
            property,
            colName,
            foreignKey,
            referenceKey,
            referenceKeyValuesMap: {},
        }

        // TODO: If colName doesn't have a unique constraint, this method will fail 
        // and you'll need to do a classic find...create.
        let results = []
        try {
            results = await tx(tablePath)
                .returning(unique([referenceKey, colName]))
                .insert(colValues.map(value => ({ [colName]: value })))
                .onConflict(colName)
                .ignore()
        } catch (err) {
            logger.error(err)
            return resp
        }

        const referenceKeyValuesMap = {}
        for (const result of (results || [])) {
            const colValue = result[colName]
            const referenceKeyValue = result[referenceKey]
            referenceKeyValuesMap[colValue] = referenceKeyValue
            this.foreignKeyMappings.set([tablePath, colName, property, colValue].join('.'), referenceKeyValue)
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
        logger.info(chalk.cyanBright(`  ${this.seedCount.toLocaleString('en-US')}`))

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
            const pkConditionsKey = inputPropertyKeys.map(k => liveObjectData[k]).join(valueSep)
            const primaryKeyConditions = indexedPkConditions[pkConditionsKey] || []
            if (!primaryKeyConditions?.length) continue

            // Merge updates by the actual primary key values.
            for (const pkConditions of primaryKeyConditions) {
                const uniquePkKey = Object.keys(pkConditions).sort().map(k => pkConditions[k]).join(valueSep)
                updates[uniquePkKey] = updates[uniquePkKey] || {
                    where: pkConditions,
                    updates: {}
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
                })
            }
        }

        try {
            if (useBulkUpdate) {
                const op = {
                    type: OpType.Update,
                    schema: this.seedSchemaName,
                    table: this.seedTableName,
                    where: bulkWhere,
                    data: bulkUpdates,
                }
                await new RunOpService(op).perform()
            } else {
                await db.transaction(async (tx) => {
                    await Promise.all(indivUpdateOps.map((op) => new RunOpService(op, tx).perform()))
                })
            }
        } catch (err) {
            throw new QueryError('update', this.seedSchemaName, this.seedTableName, err)
        }
    }

    _transformRecordsIntoFunctionInputs(
        records: StringKeyMap[],
        primaryTablePath: string
    ): StringKeyMap[] {
        const inputs = []
        for (const record of records) {
            const input = {}
            for (const colPath of this.requiredArgColPaths) {
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = [colSchemaName, colTableName].join('.')
                const inputArg = this.colPathToFunctionInputArg[colPath]
                const recordColKey = colTablePath === primaryTablePath ? colName : colPath
                input[inputArg] = record[recordColKey]
            }
            inputs.push({ ...this.defaultFilters, ...input })
        }
        return inputs
    }

    async _findInputRecordsFromAdjacentCols(
        queryConditions: StringKeyMap,
        offset: number,
        limit: number,
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
            hasArrayColumns: false
        }

        for (const colPath of this.requiredArgColPaths) {
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = [colSchemaName, colTableName].join('.')

            if (!queryConditions.hasArrayColumns && isColTypeArray(colPath)) {
                queryConditions.hasArrayColumns = true
            }

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
            } else {
                queryConditions.whereNotNull.push(colName)
            }
        }

        return queryConditions
    }

    async _getForeignInputRecordsBatch(
        tablePath: string,
        primaryKeys: string[],
        tableInputColNames: string[],
        offset: number,
        limit: number,
    ): Promise<StringKeyMap[] | null> {
        // Map col names to null.
        const whereNotNull = {}
        for (const colName of tableInputColNames) {
            whereNotNull[colName] = null
        }

        // Find all in batch where input cols names are NOT null.
        try {
            return await db
                .from(tablePath)
                .select('*')
                .orderBy(primaryKeys.sort())
                .whereNot(whereNotNull)
                .offset(offset)
                .limit(limit)
        } catch (err) {
            const [schema, table] = tablePath.split('.')
            logger.error(new QueryError('select', schema, table, err).message)
            return null
        }
    }

    _findSeedFunction() {
        for (const edgeFunction of this.liveObject.edgeFunctions) {
            // Only use a getMany function for seeding.
            if (edgeFunction.role !== LiveObjectFunctionRole.GetMany) {
                continue
            }

            const { argsMap, args } = edgeFunction
            const { seedWith } = this.seedSpec

            let allSeedWithPropertiesAcceptedAsFunctionInput = true
            for (let propertyKey of seedWith) {
                propertyKey = argsMap[propertyKey] || propertyKey

                if (!args.hasOwnProperty(propertyKey)) {
                    allSeedWithPropertiesAcceptedAsFunctionInput = false
                    break
                }
            }

            if (!allSeedWithPropertiesAcceptedAsFunctionInput) {
                continue
            }

            const reverseArgsMap = reverseMap(argsMap)
            let allRequiredInputPropertiesSatisfied = true
            for (let inputKey in args) {
                const propertyKey = reverseArgsMap[inputKey] || inputKey
                const isRequiredInput = args[inputKey]

                if (isRequiredInput && !seedWith.includes(propertyKey)) {
                    allRequiredInputPropertiesSatisfied = false
                    break
                }
            }

            if (!allRequiredInputPropertiesSatisfied) {
                continue
            }

            this.seedFunction = edgeFunction
            break
        }
    }

    _findRequiredArgColumns() {
        const { argsMap, args } = this.seedFunction
        const reverseArgsMap = reverseMap(argsMap)
        const seedWith = new Set(this.seedSpec.seedWith || [])

        const requiredArgColPaths = []
        const colPathToFunctionInputArg = {}
        for (let inputKey in args) {
            const propertyKey = reverseArgsMap[inputKey] || inputKey
            const isRequiredInput = args[inputKey]

            if (isRequiredInput || seedWith.has(propertyKey)) {
                const colPath = this.seedSpec.linkProperties[propertyKey]
                requiredArgColPaths.push(colPath)
                colPathToFunctionInputArg[colPath] = inputKey
            }
        }

        this.requiredArgColPaths = requiredArgColPaths
        this.colPathToFunctionInputArg = colPathToFunctionInputArg
    }
}

export default SeedTableService
