import logger from '../logger'
import { SeedSpec, LiveObject, EdgeFunction, LiveObjectFunctionRole, StringKeyMap, TableDataSources, Op, OpType } from '../types'
import { reverseMap, toChunks } from '../utils/formatters'
import { areColumnsEmpty } from '../db/ops'
import RunOpService from './RunOpService'
import { callSpecFunction } from '../utils/requests'
import config from '../config'
import { db } from '../db'
import { QueryError } from '../errors'
import constants from '../constants'

function getRelationshipBetweenTables(from: string, to: string): StringKeyMap {
    return {
        foreignKey: '',
        referenceKey: '',
    }
}

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

    constructor(seedSpec: SeedSpec, liveObject: LiveObject) {
        this.seedSpec = seedSpec
        this.liveObject = liveObject
        this.tableDataSources = this._getLiveObjectTableDataSources()
        this.seedFunction = null
        this.seedColNames = new Set<string>(this.seedSpec.seedColNames)
    }

    async perform() {
        logger.info(`Seeding ${this.seedTablePath}...`)

        // Find seed function to use.
        this._findSeedFunction()
        if (!this.seedFunction) throw 'Live object doesn\'t have an associated seed function.'

        // Find the required args for this function and their associated columns.
        this._getRequiredArgColumns()
        if (!this.requiredArgColPaths.length) throw 'No required-arg col-paths found.'

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

        const promises = []
        for (const tablePath in linkPropertyTableColumns) {
            const colNames = linkPropertyTableColumns[tablePath]
            promises.push(areColumnsEmpty(tablePath, colNames))
        }
        const colsEmptyResults = await Promise.all(promises)
        const allRequiredInputColsAreEmpty = colsEmptyResults.filter(v => !v).length === 0
        
        if (allRequiredInputColsAreEmpty) {
            if (linkPropertyColumnLocations.onForeignTable > 0) {
                logger.info('Can\'t seed a cross-table relationship from scratch.')
                return
            }
            if (!this.seedSpec.seedIfEmpty) {
                logger.info(`Table not configured to seed when seedIfEmpty isn't truthy: ${this.seedSpec}.`)
                return
            }
            await this._seedFromScratch()
        } else {
            linkPropertyColumnLocations.onSeedTable > 0 
                ? await this._seedWithAdjacentCols()
                : await this._seedFromForeignTable()
        }
    }

    async _runOps(ops: Op[]) {
        if (!ops.length) return

        try {
            await db.transaction(async tx => {
                await Promise.all(ops.map(op => new RunOpService(op, tx).perform()))
            })
        } catch (err) {
            logger.error(`Ops failed - ${err}`)
        }
    }

    async _seedFromScratch() {
        // Pull all live objects for this seed.
        const { data: liveObjectsData, error } = await callSpecFunction(this.seedFunction.name, [])
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

    async _seedFromForeignTable() {

    }

    async _seedWithAdjacentCols() {
        // Get all records in the seed table where each required link column has a value.
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
            const batchFunctionInputs = this._transformRecordsIntoFunctionInputs(batch)

            // Use the seed function to fetch live objects data for the batch.
            const { data: liveObjectsData, error } = await callSpecFunction(this.seedFunction.name, batchFunctionInputs)
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

    _batchInputRecords() {
        this.inputBatches = toChunks(this.inputRecords, constants.SEED_BATCH_SIZE)
    }   

    _transformRecordsIntoFunctionInputs(records: StringKeyMap[]): StringKeyMap[] {
        const inputs = []
        for (const record of records) {
            const input = {}
            for (const colPath of this.requiredArgColPaths) {
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = [colSchemaName, colTableName].join('.')
                const inputArg = this.colPathToFunctionInputArg[colPath]
                const recordColKey = colTablePath === this.seedTablePath ? colName : colPath
                input[inputArg] = record[recordColKey]
            }
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

    async _findInputRecordsFromAdjacentCols() {
        const queryConditions = this._getQueryConditionsForSeedTableInputRecords()

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

    _getQueryConditionsForSeedTableInputRecords() {
        const queryConditions = { 
            join: [],
            select: [`${this.seedTablePath}.*`],
            whereNotNull: [], 
        }

        for (const colPath of this.requiredArgColPaths) {
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = [colSchemaName, colTableName].join('.')

            if (colTablePath !== this.seedTablePath) {
                const rel = getRelationshipBetweenTables(this.seedTablePath, colTablePath)
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
}

export default SeedTableService