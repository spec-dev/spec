import { OpType, LiveObject, LiveObjectLink, StringKeyMap, TableDataSources, EdgeFunction, StringMap, LiveObjectFunctionRole } from '../types'
import { reverseMap, toMap, unique } from '../utils/formatters'
import RunOpService from './RunOpService'
import { callSpecFunction } from '../utils/requests'
import config from '../config'
import { db } from '../db'
import { QueryError } from '../errors'
import constants from '../constants'
import logger from '../logger'
import { tablesMeta, getRel } from '../db/tablesMeta'

const valueSep = '__:__'

class ResolveRecordsService {

    tablePath: string

    liveObject: LiveObject

    link: LiveObjectLink

    primaryKeyData: StringKeyMap[]

    resolveFunction: EdgeFunction | null

    inputArgColPaths: string[] = []

    colPathToFunctionInputArg: { [key: string]: string } = {}

    inputRecords: StringKeyMap[] = []

    inputPropertyKeys: string[] = []

    batchFunctionInputs: StringKeyMap[] = []

    indexedPkConditions: StringKeyMap = {}

    get schemaName(): string {
        return this.tablePath.split('.')[0]
    }

    get tableName(): string {
        return this.tablePath.split('.')[1]
    }

    get linkProperties(): StringMap {
        return toMap(this.link.linkOn)
    }

    get reverseLinkProperties(): StringMap {
        return reverseMap(this.link.linkOn)
    }

    get tableDataSources(): TableDataSources {
        return config.getLiveObjectTableDataSources(this.liveObject.id, this.tablePath)
    }

    get tablePrimaryKeys(): string[] {
        const meta = tablesMeta[this.tablePath]
        if (!meta) throw `No meta registered for table ${this.tablePath}`
        return meta.primaryKey.map(pk => pk.name)
    }

    get tableUniqueConstraint(): string[] {
        const uniqueConstaint = config.getUniqueConstraintForLink(this.liveObject.id, this.tablePath)
        if (!uniqueConstaint) throw `No unique constraint for link ${this.liveObject.id} <-> ${this.tablePath}`
        return uniqueConstaint
    }

    get updateableColNames(): Set<string> {
        const updateableColNames = []
        const linkProperties = this.linkProperties
        const tableDataSources = this.tableDataSources
        for (const property in tableDataSources) {
            if (linkProperties.hasOwnProperty(property)) continue
            for (const colName of tableDataSources[property]) {
                updateableColNames.push(colName.columnName)
            }
        }
        return new Set(updateableColNames)
    }

    get defaultFilters(): StringKeyMap {
        return this.liveObject.filterBy || {}
    }

    constructor(tablePath: string, liveObject: LiveObject, link: LiveObjectLink, primaryKeyData: StringKeyMap[]) {
        this.tablePath = tablePath
        this.liveObject = liveObject
        this.link = link
        this.primaryKeyData = primaryKeyData
        this.resolveFunction = null
    }

    async perform() {
        // Find resolve function to use.
        this._findResolveFunction()
        if (!this.resolveFunction) throw 'Live object doesn\'t have an associated resolve function.'

        // Find the input args for this function and their associated columns.
        this._findInputArgColumns()
        if (!this.inputArgColPaths.length) throw 'No input arg column paths found.'

        // Get input records for the primary key data given.
        await this._getInputRecords()
        if (!this.inputRecords.length) return

        // Create batched function inputs and an index to map live object responses -> records.
        this._createAndMapFunctionInputs()

        // Call spec function and handle response data.
        await callSpecFunction(
            this.resolveFunction, 
            this.batchFunctionInputs, 
            async data => await this._handleFunctionRespData(data as StringKeyMap[]),
        )
    }

    async _handleFunctionRespData(batch: StringKeyMap[]) {
        logger.info(`Updating batch of length ${batch.length}...`)

        const tableDataSources = this.tableDataSources
        const updateableColNames = this.updateableColNames
        const useBulkUpdate = batch.length > constants.MAX_UPDATES_BEFORE_BULK_UPDATE_USED
        const updateOps = []
        const where = []
        const updates = []
        for (const liveObjectData of batch) {
            const recordUpdates = {}
            for (const property in liveObjectData) {
                const colsWithThisPropertyAsDataSource = tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    if (updateableColNames.has(columnName)) {
                        recordUpdates[columnName] = value
                    }
                }
            }
            if (!Object.keys(recordUpdates).length) continue

            const liveObjectToPkConditionsKey = this.inputPropertyKeys.map(k => liveObjectData[k]).join(valueSep)
            const primaryKeyConditions = this.indexedPkConditions[liveObjectToPkConditionsKey] || []
            if (!primaryKeyConditions?.length) {
                logger.error(`Could not find primary keys on ${this.tablePath} for value ${liveObjectToPkConditionsKey}`)
                continue
            }

            // Bulk update.
            if (useBulkUpdate) {
                for (const pkConditions of primaryKeyConditions) {
                    where.push(pkConditions)
                    updates.push(recordUpdates)
                }
            }
            // Individual updates.
            else {
                for (const pkConditions of primaryKeyConditions) {
                    updateOps.push({
                        type: OpType.Update,
                        schema: this.schemaName,
                        table: this.tableName,
                        where: pkConditions,
                        data: recordUpdates,
                    })
                }    
            }
        }

        try {
            if (useBulkUpdate) {
                const op = {
                    type: OpType.Update,
                    schema: this.schemaName,
                    table: this.tableName,
                    where,
                    data: updates,
                }
                await new RunOpService(op).perform()
            } else {
                await db.transaction(async tx => {
                    await Promise.all(updateOps.map(op => new RunOpService(op, tx).perform()))
                })    
            }
        } catch (err) {
            throw new QueryError('update', this.schemaName, this.tableName, err)
        }
    }

    _createAndMapFunctionInputs() {
        const batchFunctionInputs = []
        const indexedPkConditions = {}
        for (const record of this.inputRecords) {
            const input = {}
            const keyComps = []
            for (const colPath of this.inputArgColPaths) {
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = [colSchemaName, colTableName].join('.')
                const inputArg = this.colPathToFunctionInputArg[colPath]
                const recordColKey = (colTablePath === this.tablePath) ? colName : colPath
                const value = record[recordColKey]
                input[inputArg] = value
                keyComps.push(value)
            }
            batchFunctionInputs.push({ ...this.defaultFilters, ...input })
            const key = keyComps.join(valueSep)
            if (!indexedPkConditions.hasOwnProperty(key)) {
                indexedPkConditions[key] = []
            } 
            const recordPrimaryKeys = {}
            for (const pk of this.tablePrimaryKeys) {
                recordPrimaryKeys[pk] = record[pk]
            }
            indexedPkConditions[key].push(recordPrimaryKeys)
        }
        this.batchFunctionInputs = batchFunctionInputs
        this.indexedPkConditions = indexedPkConditions
    }

    async _getInputRecords() {
        const queryConditions = this._buildQueryForInputRecords()

        // Start a new query on the target table.
        let query = db.from(this.tablePath)

        // Add JOIN conditions.
        for (let join of queryConditions.join) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, joinRefKey, joinForeignKey)
        }

        // Add SELECT conditions.
        query.select(queryConditions.select)

        // Add WHERE IN conditions for primary keys.
        for (let i = 0; i < queryConditions.whereIn.length; i++) {
            const [col, vals] = queryConditions.whereIn[i]
            query.whereIn(col, vals)
        }

        query.limit(this.primaryKeyData.length)

        // Perform the query.
        try {
            this.inputRecords = await query
        } catch (err) {
            throw new QueryError('select', this.schemaName, this.tableName, err)
        }
    }

    _buildQueryForInputRecords(): StringKeyMap {
        const queryConditions = { 
            join: [],
            select: [`${this.tablePath}.*`],
            whereIn: []
        }

        // Select all cols on target table + linked foreign cols.
        for (const colPath of this.inputArgColPaths) {
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = [colSchemaName, colTableName].join('.')

            if (colTablePath !== this.tablePath) {
                const rel = getRel(this.tablePath, colTablePath)
                if (!rel) throw `No rel from ${this.tablePath} -> ${colTablePath}`

                queryConditions.join.push([
                    colTableName,
                    `${colTablePath}.${rel.referenceKey}`,
                    `${this.tablePath}.${rel.foreignKey}`,
                ])

                queryConditions.select.push(`${colPath} as ${colPath}`)
            }
        }

        // Group primary keys into arrays of values for the same key.
        const primaryKeys = {}
        Object.keys(this.primaryKeyData[0]).forEach(key => {
            primaryKeys[key] = []
        })
        for (const pkData of this.primaryKeyData) {
            for (const key in pkData) {
                const val = pkData[key]
                primaryKeys[key].push(val)
            }
        }

        // Where-in each primary key
        for (const colName in primaryKeys) {
            const vals = primaryKeys[colName]
            const colPath = [this.tablePath, colName].join('.')
            queryConditions.whereIn.push([colPath, unique(vals)])
        }
        
        return queryConditions
    }

    _findResolveFunction() {
        for (const edgeFunction of this.liveObject.edgeFunctions) {
            // Only use a getOne function for resolving.
            if (edgeFunction.role !== LiveObjectFunctionRole.GetOne) {
                continue
            }

            const { argsMap, args } = edgeFunction
 
            let allUniqueByPropertiesAcceptedAsFunctionInput = true
            for (let propertyKey of this.link.uniqueBy) {
                propertyKey = argsMap[propertyKey] || propertyKey

                if (!args.hasOwnProperty(propertyKey)) {
                    allUniqueByPropertiesAcceptedAsFunctionInput = false
                    break
                }
            }

            if (!allUniqueByPropertiesAcceptedAsFunctionInput) {
                continue
            }

            const reverseArgsMap = reverseMap(argsMap)
            let allRequiredInputPropertiesSatisfied = true
            for (let inputKey in args) {
                const propertyKey = reverseArgsMap[inputKey] || inputKey
                const isRequiredInput = args[inputKey]

                if (isRequiredInput && !this.link.uniqueBy.includes(propertyKey)) {
                    allRequiredInputPropertiesSatisfied = false
                    break
                }
            }

            if (!allRequiredInputPropertiesSatisfied) {
                continue
            }

            this.resolveFunction = edgeFunction
            break
        }
    }

    /**
     * Just use all linked property columns.
     */
    _findInputArgColumns() {
        const { argsMap } = this.resolveFunction
        const linkProperties = this.linkProperties

        const inputArgColPaths = []
        const colPathToFunctionInputArg = {}
        for (let property in this.linkProperties) {
            const colPath = linkProperties[property]
            const inputArg = argsMap[property] || property
            inputArgColPaths.push(colPath)
            colPathToFunctionInputArg[colPath] = inputArg
        }

        this.inputArgColPaths = inputArgColPaths
        this.colPathToFunctionInputArg = colPathToFunctionInputArg

        // Get the live object property keys associated with each input column.
        const reverseLinkProperties = this.reverseLinkProperties
        this.inputPropertyKeys = this.inputArgColPaths.map(colPath => reverseLinkProperties[colPath])
    }
}

export default ResolveRecordsService
