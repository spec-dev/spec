import logger from '../logger'
import { SeedSpec, StringMap, LiveObject, EdgeFunction, LiveObjectFunctionRole, StringKeyMap, TableDataSources, Op, OpType, ForeignKeyConstraint } from '../types'
import { reverseMap } from '../utils/formatters'
import { areColumnsEmpty, getPrimaryKeys, getRelationshipBetweenTables, getUniqueColGroups } from '../db/ops'
import RunOpService from './RunOpService'
import { callSpecFunction } from '../utils/requests'
import config from '../config'
import { db } from '../db'
import { QueryError } from '../errors'
import constants from '../constants'

const valueSep = '__:__'

class SeedTableService {

    seedSpec: SeedSpec

    liveObject: LiveObject

    seedFunction: EdgeFunction | null

    seedColNames: Set<string>

    tableDataSources: TableDataSources

    requiredArgColPaths: string[] = []

    colPathToFunctionInputArg: { [key: string]: string } = {}

    rels: { [key: string]: ForeignKeyConstraint | null } = {}

    seedTableUniqueColGroups: string[][] = []

    seedTablePrimaryKeys: string[] = []

    get seedTablePath(): string {
        return this.seedSpec.tablePath
    }

    get seedSchemaName(): string {
        return this.seedTablePath.split('.')[0]
    }

    get seedTableName(): string {
        return this.seedTablePath.split('.')[1]
    }

    get reverseLinkProperties(): StringMap {
        return reverseMap(this.seedSpec.linkProperties)
    }

    constructor(seedSpec: SeedSpec, liveObject: LiveObject) {
        this.seedSpec = seedSpec
        this.liveObject = liveObject
        this.tableDataSources = this._getLiveObjectTableDataSources()
        this.seedFunction = null
        this.seedColNames = new Set<string>(this.seedSpec.seedColNames)
    }

    async perform() {
        // Find seed function to use.
        this._findSeedFunction()
        if (!this.seedFunction) throw 'Live object doesn\'t have an associated seed function.'

        // Find the required args for this function and their associated columns.
        this._getRequiredArgColumns()
        if (!this.requiredArgColPaths.length) throw 'No required-arg col-paths found.'

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
        const allRequiredInputColsAreEmpty = colsEmptyResults.filter(v => !v).length === 0

        // Potentially seed completely from scratch.
        if (allRequiredInputColsAreEmpty) {
            if (inputColumnLocations.onForeignTable > 0) {
                logger.warn(`${this.seedTablePath} - Can't seed a cross-table relationship from scratch.`)
                return
            }
            if (!this.seedSpec.seedIfEmpty) {
                logger.warn(`${this.seedTablePath} - Table not configured to seed -- seedIfEmpty isn't truthy.`)
                return
            }
            await this._seedFromScratch()
        }
        // Seed using the seed table as the target of the query.
        else if (inputColumnLocations.onSeedTable > 0) {
            await this._seedWithAdjacentCols()
        } 
        // TODO: This is possible, just gonna take a lot more lookup work.
        else if (inputColumnLocations.onForeignTable > 1 && uniqueTablePaths.size > 1) {
            logger.warn(`${this.seedTablePath} - Can't seed table using exclusively more than one foreign table.`)
        }
        // Seed using the foreign table as the target of the query.
        else {
            const tablePath = Object.keys(inputTableColumns)[0] as string
            const colNames = inputTableColumns[tablePath] as string[]
            await this._seedWithForeignTable(tablePath, colNames)
        }
    }

    async _seedFromScratch() {
        logger.info(`Seeding ${this.seedTablePath} from scratch...`)

        // // Pull all live objects for this seed.
        // const { data: liveObjectsData, error } = await callSpecFunction(this.seedFunction, [])
        // if (error) throw error
        // if (!liveObjectsData.length) return
        
        // // Create records data from live objects data.
        // const records = liveObjectsData.map(liveObjectData => {
        //     const record = {}
        //     for (const property in liveObjectData) {
        //         const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
        //         const value = liveObjectData[property]
        //         for (const { columnName } of colsWithThisPropertyAsDataSource) {
        //             record[columnName] = value
        //         }
        //     }
        //     return record
        // })

        // // Chunk into batches.
        // const batches = toChunks(records, constants.SEED_INPUT_BATCH_SIZE)

        // // Get seed table's unique constraints (col groups).
        // await this._findSeedTableUniqueColGroups()

        // // Insert records in batches.
        // for (let i = 0; i < batches.length; i++) {
        //     const batch = batches[i]
        //     const insertOp = {
        //         type: OpType.Insert,
        //         schema: this.seedSchemaName,
        //         table: this.seedTableName,
        //         data: batch,
        //         uniqueColGroups: this.seedTableUniqueColGroups,
        //     }

        //     // TODO: Figure out onConflict shit for upsertion using the unique columns of the seedTable.
        //     await this._runOps([insertOp])
        // }
    }

    async _seedWithAdjacentCols() {
        logger.info(`Seeding ${this.seedTablePath} from adjacent columns...`)

        // Get these once, up-front.
        const [queryConditions, _] = await Promise.all([
            this._buildQueryForSeedWithAdjacentCols(),
            await this._getSeedTablePrimaryKeys(),
        ])

        // Get the live object property keys associated with each input column.
        const reverseLinkProperties = this.reverseLinkProperties
        const inputPropertyKeys = this.requiredArgColPaths.map(colPath => reverseLinkProperties[colPath])

        // Start seeding with batches of input records.
        let offset = 0
        while (true) {
            logger.info('Running batch with offset', offset)

            // Get batch of input records from the seed table.
            const batchInputRecords = await this._findInputRecordsFromAdjacentCols(queryConditions, offset)
            if (batchInputRecords === null) {
                // TODO: Handle seed failure at this input batch.
                break
            }
            offset += batchInputRecords.length
            const isLastBatch = batchInputRecords.length < constants.SEED_INPUT_BATCH_SIZE

            const batchFunctionInputs = []
            const indexedPkConditions = {}
            for (const record of batchInputRecords) {
                const input = {}
                const keyComps = []
                for (const colPath of this.requiredArgColPaths) {
                    const [colSchemaName, colTableName, colName] = colPath.split('.')
                    const colTablePath = [colSchemaName, colTableName].join('.')
                    const inputArg = this.colPathToFunctionInputArg[colPath]
                    const recordColKey = (colTablePath === this.seedTablePath) ? colName : colPath
                    const value = record[recordColKey]
                    input[inputArg] = value
                    keyComps.push(value)
                }
                batchFunctionInputs.push(input)
                const key = keyComps.join(valueSep)
                if (!indexedPkConditions.hasOwnProperty(key)) {
                    indexedPkConditions[key] = []
                } 
                const recordPrimaryKeys = {}
                for (const pk of this.seedTablePrimaryKeys) {
                    recordPrimaryKeys[pk] = record[pk]
                }
                indexedPkConditions[key].push(recordPrimaryKeys)
            }

            // Callback to use when a batch of response data is available.
            const onFunctionRespData = async data => await this._handleSpecFunctionRespForAdjacentColsSeed(
                data,
                inputPropertyKeys,
                indexedPkConditions,
            )

            // Call spec function and handle response data.
            try {
                await callSpecFunction(this.seedFunction, batchFunctionInputs, onFunctionRespData)
            } catch (err) {
                // TODO: Handle seed failure at this input batch.
                break
            }

            if (isLastBatch) break
        }
    }

    async _seedWithForeignTable(foreignTablePath: string, inputColNames: string[]) {
        logger.info(`Seeding ${this.seedTablePath} with foreign table ${foreignTablePath}...`)

        // Get seed table -> foreign table relationship.
        const rel = await this._getRel(this.seedTablePath, foreignTablePath)
        if (!rel) throw `No relationship ${this.seedTablePath} -> ${foreignTablePath} exists.`

        // Get the live object property keys associated with each input column.
        const reverseLinkProperties = this.reverseLinkProperties
        const inputPropertyKeys = inputColNames.map(colName => reverseLinkProperties[`${foreignTablePath}.${colName}`])

        // Get these once, up-front.
        const [foreignTablePrimaryKeys, _] = await Promise.all([
            getPrimaryKeys(foreignTablePath),
            this._findSeedTableUniqueColGroups(),
        ])

        // Start seeding with batches of input records.
        let offset = 0
        while (true) {
            logger.info('Running batch with offset', offset)

            // Get batch of input records from the foreign table.
            const batchInputRecords = await this._getInputRecordsBatch(
                foreignTablePath,
                foreignTablePrimaryKeys as string[],
                inputColNames,
                offset,
            )
            if (batchInputRecords === null) {
                // TODO: Handle seed failure at this input batch.
                break
            }
            offset += batchInputRecords.length
            const isLastBatch = batchInputRecords.length < constants.SEED_INPUT_BATCH_SIZE

            // Map the input records to their reference key value, so that records being added
            // to the seed table later can easily find/assign their foreign keys. 
            const referenceKeyValues = {}
            for (const record of batchInputRecords) {
                const key = inputColNames.map(colName => record[colName]).join(valueSep)
                if (referenceKeyValues.hasOwnProperty(key)) continue
                referenceKeyValues[key] = record[rel.referenceKey]
            }
            
            // Transform the records into seed-function inputs.
            const batchFunctionInputs = this._transformRecordsIntoFunctionInputs(batchInputRecords, foreignTablePath)

            // Callback to use when a batch of response data is available.
            const onFunctionRespData = async data => await this._handleSpecFunctionRespForForeignTableSeed(
                data,
                rel,
                inputPropertyKeys,
                foreignTablePath,
                referenceKeyValues,
            )

            // Call spec function and handle response data.
            try {
                await callSpecFunction(this.seedFunction, batchFunctionInputs, onFunctionRespData)
            } catch (err) {
                // TODO: Handle seed failure at this input batch.
                break
            }

            if (isLastBatch) break
        }
    }

    async _handleSpecFunctionRespForForeignTableSeed(
        batch: StringKeyMap[], 
        rel: ForeignKeyConstraint,
        inputPropertyKeys: string[],
        foreignTablePath: string,
        referenceKeyValues: StringKeyMap,
    ) {
        const upsertRecords = []
        for (const liveObjectData of batch) {
            // Format a seed table record for this live object data.
            const upsertRecord = {}
            for (const property in liveObjectData) {
                const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    upsertRecord[columnName] = value
                }
            }

            // Find and set the reference key for the foreign table.
            const uniqueInputRecordKey = inputPropertyKeys.map(propertyKey => liveObjectData[propertyKey]).join(valueSep)
            if (!referenceKeyValues.hasOwnProperty(uniqueInputRecordKey)) {
                logger.error(`Could not find reference key on foreign table ${foreignTablePath} for value ${uniqueInputRecordKey}`)
                continue
            }
            const referenceKeyValue = referenceKeyValues[uniqueInputRecordKey]
            upsertRecord[rel.foreignKey] = referenceKeyValue
            upsertRecords.push(upsertRecord)
        }

        const upsertBatchOp = {
            type: OpType.Insert,
            schema: this.seedSchemaName,
            table: this.seedTableName,
            data: upsertRecords,
            uniqueColGroups: this.seedTableUniqueColGroups,
        }

        try {
            await db.transaction(async tx => {
                await new RunOpService(upsertBatchOp, tx).perform()
            })
        } catch (err) {
            throw new QueryError('upsert', this.seedSchemaName, this.seedTableName, err)
        }
    }

    async _handleSpecFunctionRespForAdjacentColsSeed(
        batch: StringKeyMap[], 
        inputPropertyKeys: string[],
        indexedPkConditions: StringKeyMap, 
    ) {
        const updateOps = []
        for (const liveObjectData of batch) {
            // Format a seed table record for this live object data.
            const updates = {}
            for (const property in liveObjectData) {
                const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    if (this.seedColNames.has(columnName)) {
                        updates[columnName] = value
                    }
                }
            }

            const liveObjectToPkConditionsKey = inputPropertyKeys.map(k => liveObjectData[k]).join(valueSep)
            const primaryKeyConditions = indexedPkConditions[liveObjectToPkConditionsKey] || []
            if (!primaryKeyConditions?.length) {
                logger.error(`Could not find primary keys on ${this.seedTablePath} for value ${liveObjectToPkConditionsKey}`)
                continue
            }

            for (const pkConditions of primaryKeyConditions) {
                updateOps.push({
                    type: OpType.Update,
                    schema: this.seedSchemaName,
                    table: this.seedTableName,
                    where: pkConditions,
                    data: updates,
                })
            }
        }

        try {
            await db.transaction(async tx => {
                await Promise.all(updateOps.map(op => new RunOpService(op, tx).perform()))
            })
        } catch (err) {
            throw new QueryError('update', this.seedSchemaName, this.seedTableName, err)
        }
    }

    _transformRecordsIntoFunctionInputs(records: StringKeyMap[], primaryTablePath: string): StringKeyMap[] {
        const inputs = []
        for (const record of records) {
            const input = {}
            for (const colPath of this.requiredArgColPaths) {
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = [colSchemaName, colTableName].join('.')
                const inputArg = this.colPathToFunctionInputArg[colPath]
                const recordColKey = (colTablePath === primaryTablePath) ? colName : colPath
                input[inputArg] = record[recordColKey]
            }
            inputs.push(input)
        }
        return inputs
    }

    async _findInputRecordsFromAdjacentCols(queryConditions: StringKeyMap, offset: number): Promise<StringKeyMap[]> {
        // Start a new query on the table this live object is linked to.
        let query = db.from(this.seedTablePath)

        // Add JOIN conditions.
        for (let join of queryConditions.join) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, joinRefKey, joinForeignKey)
        }

        // Add SELECT conditions and order by primary keys.
        query.select(queryConditions.select).orderBy(this.seedTablePrimaryKeys)

        // Add WHERE NOT NULL conditions.
        const whereNotNull = {}
        for (let i = 0; i < queryConditions.whereNotNull.length; i++) {
            whereNotNull[queryConditions.whereNotNull[i]] = null
        }
        query.whereNot(whereNotNull)

        // Add offset/limit for batching.
        query.offset(offset).limit(constants.SEED_INPUT_BATCH_SIZE)

        // Perform the query.
        try {
            return await query
        } catch (err) {
            throw new QueryError('select', this.seedSchemaName, this.seedTableName, err)
        }
    }

    async _buildQueryForSeedWithAdjacentCols(): Promise<StringKeyMap> {
        const queryConditions = { 
            join: [],
            select: [`${this.seedTablePath}.*`],
            whereNotNull: [], 
        }

        for (const colPath of this.requiredArgColPaths) {
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = [colSchemaName, colTableName].join('.')

            if (colTablePath !== this.seedTablePath) {
                const rel = await this._getRel(this.seedTablePath, colTablePath)
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

    async _getInputRecordsBatch(
        tablePath: string, 
        primaryKeys: string[], 
        tableInputColNames: string[], 
        offset: number,
    ): Promise<StringKeyMap[] | null> {
        // Map col names to null.
        const whereNotNull = {}
        for (const colName of tableInputColNames) {
            whereNotNull[colName] = null
        }

        // Find all in batch where input cols names are NOT null.
        try {
            return await db.from(tablePath)
                .select('*')
                .orderBy(primaryKeys)
                .whereNot(whereNotNull)
                .offset(offset)
                .limit(constants.SEED_INPUT_BATCH_SIZE)
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

    async _findSeedTableUniqueColGroups() {
        this.seedTableUniqueColGroups = await getUniqueColGroups(this.seedTablePath)
    }

    _getRequiredArgColumns() {
        const { argsMap, args } = this.seedFunction
        const reverseArgsMap = reverseMap(argsMap)

        const requiredArgColPaths = []
        const colPathToFunctionInputArg = {}
        for (let inputKey in args) {
            const propertyKey = reverseArgsMap[inputKey] || inputKey
            const isRequiredInput = args[inputKey]

            if (isRequiredInput) {
                const colPath = this.seedSpec.linkProperties[propertyKey]
                requiredArgColPaths.push(colPath)
                colPathToFunctionInputArg[colPath] = inputKey
            }
        }

        this.requiredArgColPaths = requiredArgColPaths
        this.colPathToFunctionInputArg = colPathToFunctionInputArg
    }

    _getLiveObjectTableDataSources(): TableDataSources {
        const dataSourcesInTable = config.getDataSourcesForTable(this.seedSchemaName, this.seedTableName) || {}
        const tableDataSourcesForThisLiveObject = {}

        // Basically just recreate the map, but filtering out the data sources that 
        // aren't associated with our live object. Additionally, use just the live 
        // object property as the new key (removing the live object id).
        for (let key in dataSourcesInTable) {
            const [liveObjectId, property] = key.split(':')
            if (liveObjectId !== this.liveObject.id) continue
            tableDataSourcesForThisLiveObject[property] = dataSourcesInTable[key]
        }

        return tableDataSourcesForThisLiveObject
    }

    async _getRel(tablePath: string, foreignTablePath: string): Promise<ForeignKeyConstraint | null> {
        const key = [tablePath, foreignTablePath].join(':')

        if (this.rels.hasOwnProperty(key)) {
            return this.rels[key]
        }
        
        const rel = await getRelationshipBetweenTables(tablePath, foreignTablePath)
        this.rels[key] = rel
        return rel
    }

    async _getSeedTablePrimaryKeys() {
        this.seedTablePrimaryKeys = await getPrimaryKeys(this.seedTablePath) as string[]
        if (!this.seedTablePrimaryKeys.length) throw `Primary keys could not determined for ${this.seedTablePath}`
    }
}

export default SeedTableService