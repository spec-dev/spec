import logger from '../logger'
import { SeedSpec, LiveObject, EdgeFunction, LiveObjectFunctionRole, StringKeyMap, TableDataSources, Op, OpType, ForeignKeyConstraint } from '../types'
import { reverseMap, toChunks } from '../utils/formatters'
import { areColumnsEmpty, getRelationshipBetweenTables } from '../db/ops'
import RunOpService from './RunOpService'
import { callSpecFunction } from '../utils/requests'
import config from '../config'
import { db } from '../db'
import { QueryError } from '../errors'
import constants from '../constants'

class SeedTableService {

    seedSpec: SeedSpec

    liveObject: LiveObject

    seedFunction: EdgeFunction | null

    seedColNames: Set<string>

    tableDataSources: TableDataSources

    requiredArgColPaths: string[] = []

    colPathToFunctionInputArg: { [key: string]: string } = {}

    inputRecords: StringKeyMap[] = []

    inputBatches: StringKeyMap[][] = []

    rels: { [key: string]: ForeignKeyConstraint | null } = {}

    get seedTablePath(): string {
        return this.seedSpec.tablePath
    }

    get seedSchemaName(): string {
        return this.seedTablePath.split('.')[0]
    }

    get seedTableName(): string {
        return this.seedTablePath.split('.')[1]
    }

    get seedTablePrimaryKeys(): string[] {
        return ['id']
    }

    get seedFunctionName(): string {
        return this.seedFunction.name
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
        const linkPropertyTableColumns = {}
        const linkPropertyColumnLocations = { onSeedTable: 0, onForeignTable: 0 }
        for (const colPath of this.requiredArgColPaths) {
            const [schemaName, tableName, colName] = colPath.split('.')
            const tablePath = [schemaName, tableName].join('.')
            
            tablePath === this.seedTablePath
                ? linkPropertyColumnLocations.onSeedTable++
                : linkPropertyColumnLocations.onForeignTable++

            if (!linkPropertyTableColumns.hasOwnProperty(tablePath)) {
                linkPropertyTableColumns[tablePath] = []
            }
            linkPropertyTableColumns[tablePath].push(colName)
        }

        // TODO: Break out.
        const promises = []
        for (const tablePath in linkPropertyTableColumns) {
            const colNames = linkPropertyTableColumns[tablePath]
            promises.push(areColumnsEmpty(tablePath, colNames))
        }
        const colsEmptyResults = await Promise.all(promises)
        const allRequiredInputColsAreEmpty = colsEmptyResults.filter(v => !v).length === 0

        if (allRequiredInputColsAreEmpty) {
            if (linkPropertyColumnLocations.onForeignTable > 0) {
                logger.warn(`${this.seedTablePath} - Can't seed a cross-table relationship from scratch.`)
                return
            }
            if (!this.seedSpec.seedIfEmpty) {
                logger.warn(`${this.seedTablePath} - Table not configured to seed -- seedIfEmpty isn't truthy.`)
                return
            }
            await this._seedFromScratch()
        } else if (linkPropertyColumnLocations.onSeedTable > 0) {
            await this._seedWithAdjacentCols()
        } else if (linkPropertyColumnLocations.onForeignTable > 1) {
            logger.warn(`${this.seedTablePath} - Can't seed table using exclusively more than one foreign table.`)
        } else {
            const tablePath = Object.keys(linkPropertyTableColumns)[0] as string
            const colNames = linkPropertyTableColumns[tablePath] as string[]
            await this._seedFromForeignTable(tablePath, colNames)
        }
    }

    async _seedFromScratch() {
        logger.info(`Seeding ${this.seedTablePath} from scratch...`)

        // Pull all live objects for this seed.
        const { data: liveObjectsData, error } = await callSpecFunction(this.seedFunctionName, [])
        if (error) throw error
        if (!liveObjectsData.length) return
        
        // Create records data from live objects data.
        const records = liveObjectsData.map(liveObjectData => {
            const record = {}
            for (const property in liveObjectData) {
                const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    record[columnName] = value
                }
            }
            return record
        })

        // Chunk into batches.
        const batches = toChunks(records, constants.SEED_BATCH_SIZE)

        // Insert records in batches.
        for (let i = 0; i < batches.length; i++) {
            const batch = batches[i]
            const insertOp = {
                type: OpType.Insert,
                schema: this.seedSchemaName,
                table: this.seedTableName,
                data: batch,
            }

            // TODO: Figure out onConflict shit for upsertion using the unique columns of the seedTable.
            await this._runOps([insertOp])
        }
    }

    async _seedWithAdjacentCols() {
        logger.info(`Seeding ${this.seedTablePath} from adjacent columns...`)

        // Get all records in the seed table where each required link column is NOT NULL.
        await this._findInputRecordsFromAdjacentCols()
        if (!this.inputRecords.length) {
            logger.info('Found no adjacent-column input records to seed with...')
            return
        }
        
        // Group input records into batches.
        this._batchInputRecords()

        // Process each batch.
        for (let i = 0; i < this.inputBatches.length; i++) {
            const batch = this.inputBatches[i]

            // Transform the records into function input payloads.
            const batchFunctionInputs = this._transformRecordsIntoFunctionInputs(batch, this.seedTablePath)

            // Use the seed function to fetch live objects data for the batch.
            const { data: liveObjectsData, error } = await callSpecFunction(this.seedFunctionName, batchFunctionInputs)
            if (error || !liveObjectsData.length) continue
            if (liveObjectsData.length !== batch.length) {
                logger.error(`Seed function response length mismatch: ${liveObjectsData.length} vs. ${batch.length}`)
                continue
            }

            // Use the function response data (live objects data) to generate record-update ops.
            const updateOps = this._generateUpdateOpsForSeedTableBatch(batch, liveObjectsData)
            await this._runOps(updateOps)
        }
    }

    async _seedFromForeignTable(foreignTablePath: string, linkColNames: string[]) {
        logger.info(`Seeding ${this.seedTablePath} from foreign table ${foreignTablePath}...`)

        // Get seed table -> foreign table relationship.
        const rel = await this._getRel(this.seedTablePath, foreignTablePath)
        if (!rel) throw `No relationship ${this.seedTablePath} -> ${foreignTablePath} exists.`

        // Get all records in the foreign table where each required link column is NOT NULL.
        await this._findInputRecordsFromForeignTable(foreignTablePath, linkColNames)
        if (!this.inputRecords.length) {
            logger.info('Found no foreign table input records to seed with...')
            return
        }

        // Group input records into batches.
        this._batchInputRecords()

        // Process each batch.
        for (let i = 0; i < this.inputBatches.length; i++) {
            const batch = this.inputBatches[i]

            // Transform the records into function input payloads.
            const batchFunctionInputs = this._transformRecordsIntoFunctionInputs(batch, foreignTablePath)

            // Use the seed function to fetch live objects data for the batch.
            const { data: liveObjectsData, error } = await callSpecFunction(this.seedFunctionName, batchFunctionInputs)
            if (error || !liveObjectsData.length) continue
            if (liveObjectsData.length !== batch.length) {
                logger.error(`Seed function response length mismatch: ${liveObjectsData.length} vs. ${batch.length}`)
                continue
            }

            // Use the function response data (live objects data) to generate upsert ops.
            const ops = await this._generateOpsToUpsertLiveObjectRecords(batch, rel, liveObjectsData)
            await this._runOps(ops)
        }
    }

    async _runOps(ops: Op[]) {
        if (!ops.length) return

        try {
            await db.transaction(async tx => {
                await Promise.all(ops.map(op => new RunOpService(op, tx).perform()))
            })
        } catch (err) {
            logger.error(`Seed ops failed: ${err}`)
        }
    }

    _batchInputRecords() {
        this.inputBatches = toChunks(this.inputRecords, constants.SEED_BATCH_SIZE)
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

    _generateUpdateOpsForSeedTableBatch(batch: StringKeyMap[], liveObjectsData: StringKeyMap[]): Op[] {
        const ops = []
        for (let i = 0; i < batch.length; i++) {
            const record = batch[i]
            const liveObjectData = liveObjectsData[i]

            const recordUpdates = {}
            for (const property in liveObjectData) {
                const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    if (this.seedColNames.has(columnName)) {
                        recordUpdates[columnName] = value
                    }
                }
            }
            if (!Object.keys(recordUpdates).length) continue

            // Create the lookup/where conditions for the update.
            // These will just be the primary keys / values of this record.
            const whereConditions = {}
            for (let primaryKey of this.seedTablePrimaryKeys) {
                whereConditions[primaryKey] = record[primaryKey]
            }

            ops.push({
                type: OpType.Update,
                schema: this.seedSchemaName,
                table: this.seedTableName,
                where: whereConditions,
                data: recordUpdates,
            })
        }

        return ops
    }

    async _generateOpsToUpsertLiveObjectRecords(
        foreignInputRecords: StringKeyMap[],
        foreignRel: StringKeyMap,
        liveObjectsData: StringKeyMap[],
    ): Promise<Op[]> {
        const existingLiveObjectRecords = await this._findExistingLiveObjectRecords(liveObjectsData.flat())
        const properties = this.seedSpec.linkProperties
        const sortedPropertyKeys: string[] = Object.keys(properties).sort()
        const { foreignKey, referenceKey } = foreignRel

        const existingRecordsByLinkedPropertyValues: { [key: string]: StringKeyMap[] } = {}
        for (const record of existingLiveObjectRecords) {
            const key = sortedPropertyKeys.map(property => {
                const colPath = properties[property]
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = `${colSchemaName}.${colTableName}`
                const colKey = (colTablePath === this.seedTablePath) ? colName : colPath
                return record[colKey]
            }).join(':')
            if (!existingRecordsByLinkedPropertyValues.hasOwnProperty(key)) {
                existingRecordsByLinkedPropertyValues[key] = []
            }
            existingRecordsByLinkedPropertyValues[key].push(record)
        }

        const ops = []
        for (let i = 0; i < liveObjectsData.length; i++) {
            const foreignInputRecord = foreignInputRecords[i]
            const entry = liveObjectsData[i]
            const liveObjectDataGroup = Array.isArray(entry) ? entry : [entry]

            for (const liveObjectData of liveObjectDataGroup) {
                const key = sortedPropertyKeys.map(property => liveObjectData[property]).join(':')
                const existingRecords = existingRecordsByLinkedPropertyValues[key] || []
    
                if (existingRecords.length > 0) {
                    for (const existingRecord of existingRecords) {
                        const recordUpdates = {}
                        for (const property in liveObjectData) {
                            const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
                            const value = liveObjectData[property]
                            for (const { columnName } of colsWithThisPropertyAsDataSource) {
                                if (this.seedColNames.has(columnName)) {
                                    recordUpdates[columnName] = value
                                }
                            }
                        }
                        if (!Object.keys(recordUpdates).length) continue
            
                        // Create the lookup/where conditions for the update.
                        // These will just be the primary keys / values of this record.
                        const whereConditions = {}
                        for (let primaryKey of this.seedTablePrimaryKeys) {
                            whereConditions[primaryKey] = existingRecord[primaryKey]
                        }
                        ops.push({
                            type: OpType.Update,
                            schema: this.seedSchemaName,
                            table: this.seedTableName,
                            where: whereConditions,
                            data: recordUpdates,
                        })    
                    }
                } else {
                    const newRecord = {}
                    for (const property in liveObjectData) {
                        const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
                        const value = liveObjectData[property]
                        for (const { columnName } of colsWithThisPropertyAsDataSource) {
                            newRecord[columnName] = value
                        }
                    }
                    newRecord[foreignKey] = foreignInputRecord[referenceKey]
                    ops.push({
                        type: OpType.Insert,
                        schema: this.seedSchemaName,
                        table: this.seedTableName,
                        data: newRecord,
                    })
                }    
            }
        }

        return ops
    }

    async _findRecords(tablePath: string, joinConditions: string[][], whereConditions: any[][]): Promise<StringKeyMap[]> {
        let query = db.from(tablePath)

        // Add JOIN conditions.
        for (let join of joinConditions) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, joinRefKey, joinForeignKey)
        }

        // Add WHERE conditions.
        for (let i = 0; i < whereConditions.length; i++) {
            const [col, val] = whereConditions[i]
            i ? query.andWhere(col, val) : query.where(col, val)
        }

        try {
            return await query
        } catch (err) {
            const [schema, table] = tablePath.split('.')
            throw new QueryError('select', schema, table, err)
        }
    }

    async _findInputRecordsFromAdjacentCols() {
        const queryConditions = await this._getQueryConditionsForSeedTableInputRecords()

        // Start a new query on the table this live object is linked to.
        let query = db.from(this.seedTablePath)

        // Add JOIN conditions.
        for (let join of queryConditions.join) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, joinRefKey, joinForeignKey)
        }

        // Add SELECT conditions.
        query.select(queryConditions.select)

        // Add WHERE NOT NULL conditions.
        const whereNotNull = {}
        for (let i = 0; i < queryConditions.whereNotNull.length; i++) {
            whereNotNull[queryConditions.whereNotNull[i]] = null
        }
        query.whereNot(whereNotNull)

        // Perform the query.
        try {
            this.inputRecords = await query
        } catch (err) {
            throw new QueryError('select', this.seedSchemaName, this.seedTableName, err)
        }
    }

    async _findExistingLiveObjectRecords(liveObjectsData: StringKeyMap[]): Promise<StringKeyMap[]> {
        const queryConditions = await this._getQueryConditionsForExistingLiveObjectRecords(liveObjectsData)

        // Start a new query on the table this live object is linked to.
        let query = db.from(this.seedTablePath)

        // Add JOIN conditions.
        for (let join of queryConditions.join) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, joinRefKey, joinForeignKey)
        }

        // Add SELECT conditions.
        query.select(queryConditions.select)

        // Add WHERE conditions.
        for (let i = 0; i < queryConditions.where.length; i++) {
            const params = queryConditions.where[i]
            i ? query.orWhere(params) : query.where(params)
        }

        try {
            return await query
        } catch (err) {
            throw new QueryError('select', this.seedSchemaName, this.seedTableName, err)
        }
    }

    async _getQueryConditionsForSeedTableInputRecords(): Promise<StringKeyMap> {
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

    async _getQueryConditionsForExistingLiveObjectRecords(liveObjectsData: StringKeyMap[]): Promise<StringKeyMap> {
        const queryConditions = { 
            join: [],
            select: [`${this.seedTablePath}.*`],
            where: [], 
        }
        const properties = this.seedSpec.linkProperties
        const tablePath = this.seedTablePath

        for (const property in properties) {
            const colPath = properties[property]
            const [colSchemaName, colTableName, _] = colPath.split('.')
            const colTablePath = `${colSchemaName}.${colTableName}`
    
            // Handle foreign tables.
            if (colTablePath !== tablePath) {
                const rel = await this._getRel(tablePath, colTablePath)
                if (!rel) throw `No rel from ${tablePath} -> ${colTablePath}`

                queryConditions.join.push([
                    colTableName,
                    `${colTablePath}.${rel.referenceKey}`,
                    `${tablePath}.${rel.foreignKey}`,
                ])
                queryConditions.select.push(`${colPath} as ${colPath}`)
            }
        }
        
        for (const liveObjectData of liveObjectsData) {
            const whereConditions = {}
            for (const property in properties) {
                const colPath = properties[property]
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = `${colSchemaName}.${colTableName}`
                const colKey = (colTablePath === this.seedTablePath) ? colName : colPath
                whereConditions[colKey] = liveObjectData[property]
            }
            queryConditions.where.push(whereConditions)
        }

        return queryConditions
    }

    async _findInputRecordsFromForeignTable(foreignTablePath: string, linkColNames: string[]) {
        // Map col names to null.
        const whereNotNull = {}
        for (const colName of linkColNames) {
            whereNotNull[colName] = null
        }

        // Find all where NOT null.
        try {
            this.inputRecords = await db.from(foreignTablePath)
                .select('*')
                .whereNot(whereNotNull)
        } catch (err) {
            const [schema, table] = foreignTablePath.split('.')
            throw new QueryError('select', schema, table, err)
        }
    }

    _findSeedFunction() {
        for (const edgeFunction of this.liveObject.edgeFunctions) {
            // Only use a getMany function for seeding.
            if (edgeFunction.role !== LiveObjectFunctionRole.GetMany) {
                continue
            }

            const { argsMap, args } = edgeFunction
            const { linkProperties } = this.seedSpec
 
            let allLinkedPropertiesAcceptedAsFunctionInput = true
            for (let propertyKey in linkProperties) {
                propertyKey = argsMap[propertyKey] || propertyKey

                if (!args.hasOwnProperty(propertyKey)) {
                    allLinkedPropertiesAcceptedAsFunctionInput = false
                    break
                }
            }

            if (!allLinkedPropertiesAcceptedAsFunctionInput) {
                continue
            }

            const reverseArgsMap = reverseMap(argsMap)
            let allRequiredInputPropertiesSatisfied = true
            for (let inputKey in args) {
                const propertyKey = reverseArgsMap[inputKey] || inputKey
                const isRequiredInput = args[inputKey]

                if (isRequiredInput && !linkProperties.hasOwnProperty(propertyKey)) {
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
        const allTableDataSources = config.getDataSourcesForTable(this.seedSchemaName, this.seedTableName) || {}
        const tableDataSourcesForThisLiveObject = {}

        // Basically just recreate the map, but filtering out the data sources that 
        // aren't associated with our live object. Additionally, use just the live 
        // object property as the new key (removing the live object id).
        for (let key in allTableDataSources) {
            const [liveObjectId, property] = key.split(':')
            if (liveObjectId !== this.liveObject.id) continue
            tableDataSourcesForThisLiveObject[property] = allTableDataSources[key]
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
}

export default SeedTableService