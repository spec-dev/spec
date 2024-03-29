import {
    OpType,
    LiveObject,
    StringKeyMap,
    TableDataSources,
    StringMap,
    ResolveRecordsSpec,
    ColumnDefaultsConfig,
    EnrichedLink,
} from '../types'
import { reverseMap, toMap, unique, getCombinations, groupByKeys } from '../utils/formatters'
import RunOpService from './RunOpService'
import { querySharedTable } from '../shared-tables/client'
import config from '../config'
import { db } from '../db'
import { QueryError } from '../errors'
import { constants } from '../constants'
import logger from '../logger'
import { tablesMeta, getRel } from '../db/tablesMeta'
import chalk from 'chalk'
import { withDeadlockProtection } from '../utils/db'

const valueSep = '__:__'

class ResolveRecordsService {
    tablePath: string

    liveObject: LiveObject

    enrichedLink: EnrichedLink

    primaryKeyData: StringKeyMap[]

    seedCursorId: string

    cursor: number

    seedCount: number = 0

    inputArgColPaths: string[] = []

    colPathToFunctionInputArg: { [key: string]: string } = {}

    inputRecords: StringKeyMap[] = []

    inputPropertyKeys: string[] = []

    batchFunctionInputs: StringKeyMap = {}

    indexedPkConditions: StringKeyMap = {}

    liveTableColumns: string[]

    tableDataSources: TableDataSources

    defaultColumnValues: { [key: string]: ColumnDefaultsConfig }

    primaryTimestampColumn: string | null

    get schemaName(): string {
        return this.tablePath.split('.')[0]
    }

    get tableName(): string {
        return this.tablePath.split('.')[1]
    }

    get linkProperties(): StringMap {
        return toMap(this.enrichedLink.linkOn)
    }

    get reverseLinkProperties(): StringMap {
        return reverseMap(this.enrichedLink.linkOn)
    }

    get tablePrimaryKeys(): string[] {
        const meta = tablesMeta[this.tablePath]
        if (!meta) throw `No meta registered for table ${this.tablePath}`
        return meta.primaryKey.map((pk) => pk.name)
    }

    get tableUniqueConstraint(): string[] {
        return this.enrichedLink.uniqueConstraint
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

    get linkUniqueByProperties(): string[] {
        return this.enrichedLink.uniqueByProperties
    }

    get primaryTimestampProperty(): string | null {
        return this.liveObject.config?.primaryTimestampProperty || null
    }

    get sharedTablePath(): string {
        return this.liveObject.config.table
    }

    constructor(
        resolveRecordsSpec: ResolveRecordsSpec,
        liveObject: LiveObject,
        seedCursorId: string,
        cursor: number
    ) {
        this.tablePath = resolveRecordsSpec.tablePath
        this.liveObject = liveObject
        this.primaryKeyData = resolveRecordsSpec.primaryKeyData
        this.seedCursorId = seedCursorId
        this.cursor = cursor
        this.liveTableColumns = Object.keys(config.getTable(this.schemaName, this.tableName) || {})
        this.tableDataSources = config.getLiveObjectTableDataSources(
            this.liveObject.id,
            this.tablePath
        )

        this.enrichedLink = config.getEnrichedLink(this.liveObject.id, this.tablePath)
        if (!this.enrichedLink)
            throw `No enriched link found for link ${this.liveObject.id} <> ${this.tablePath}`

        this.primaryTimestampColumn = this.primaryTimestampProperty
            ? (this.tableDataSources[this.primaryTimestampProperty] || [])[0]?.columnName || null
            : null

        this.defaultColumnValues = config.getDefaultColumnValuesForTable(this.tablePath)
    }

    async perform() {
        // Find the input args for this function and their associated columns.
        this._findInputArgColumns()
        if (!this.inputArgColPaths.length) throw 'No input arg column paths found.'

        // Get input records for the primary key data given.
        await this._getInputRecords()
        if (!this.inputRecords.length) return

        // Create batched function inputs and an index to map live object responses -> records.
        this._createAndMapFunctionInputs()

        logger.info(
            chalk.cyanBright(
                `Resolving live data for ${this.inputRecords.length} records in ${this.tablePath}... `
            )
        )

        // Call spec function and handle response data.
        const sharedErrorContext = { error: null }
        const t0 = performance.now()
        try {
            await querySharedTable(
                this.sharedTablePath,
                this.batchFunctionInputs,
                async (data) =>
                    await this._handleFunctionRespData(data as StringKeyMap[]).catch((err) => {
                        sharedErrorContext.error = err
                    }),
                sharedErrorContext,
                {},
                true
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
                `Updated ${this.seedCount.toLocaleString(
                    'en-US'
                )} records in ${seconds} seconds (${rate.toLocaleString('en-US')} rows/s)`
            )
        )
    }

    async _handleFunctionRespData(batch: StringKeyMap[]) {
        this.seedCount += batch.length
        logger.info(chalk.cyanBright(`  ${this.seedCount.toLocaleString('en-US')}`))

        const tableDataSources = this.tableDataSources
        const updateableColNames = this.updateableColNames
        const updates: StringKeyMap = {}

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

            // Find the primary key groups to apply the updates to.
            const pkConditionsKey = this.inputPropertyKeys
                .map((k) => liveObjectData[k])
                .join(valueSep)
            const primaryKeyConditions = this.indexedPkConditions[pkConditionsKey] || []
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
                    schema: this.schemaName,
                    table: this.tableName,
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
                    const updateOp = {
                        type: OpType.Update,
                        schema: this.schemaName,
                        table: this.tableName,
                        where: bulkWhere,
                        data: bulkUpdates,
                        liveTableColumns: this.liveTableColumns,
                        primaryTimestampColumn: this.primaryTimestampColumn,
                        defaultColumnValues: this.defaultColumnValues,
                    }
                    await new RunOpService(updateOp).perform()
                } else {
                    await db.transaction(async (tx) => {
                        await Promise.all(
                            indivUpdateOps.map((updateOp) =>
                                new RunOpService(updateOp, tx).perform()
                            )
                        )
                    })
                }
            } catch (err) {
                throw new QueryError('update', this.schemaName, this.tableName, err)
            }
        }

        await withDeadlockProtection(op)
    }

    _createAndMapFunctionInputs() {
        const batchFunctionInputs = []
        const indexedPkConditions = {}

        for (const record of this.inputRecords) {
            const input = {}
            const colValues = []

            for (const colPath of this.inputArgColPaths) {
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = [colSchemaName, colTableName].join('.')
                const inputArg = this.colPathToFunctionInputArg[colPath]
                const recordColKey = colTablePath === this.tablePath ? colName : colPath
                const value = record[recordColKey]
                input[inputArg] = value
                colValues.push(value)
            }

            // NOTE: commented out until you upgrade to new version of "filterBy"
            // batchFunctionInputs.push({ ...this.defaultFilters, ...input })

            const recordPrimaryKeys = {}
            for (const pk of this.tablePrimaryKeys) {
                recordPrimaryKeys[pk] = record[pk]
            }

            const colValueOptions = getCombinations(colValues)
            for (const valueOptions of colValueOptions) {
                const key = valueOptions.join(valueSep)
                indexedPkConditions[key] = indexedPkConditions[key] || []
                indexedPkConditions[key].push(recordPrimaryKeys)
            }
        }

        this.batchFunctionInputs = groupByKeys(batchFunctionInputs)
        this.indexedPkConditions = indexedPkConditions
    }

    async _getInputRecords() {
        const queryConditions = this._buildQueryForInputRecords()

        // Start a new query on the target table.
        let query = db.from(this.tablePath)

        // Add JOIN conditions.
        for (let join of queryConditions.join) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, (builder) => {
                for (let i = 0; i < joinRefKey.length; i++) {
                    i === 0
                        ? builder.on(joinRefKey[i], joinForeignKey[i])
                        : builder.andOn(joinRefKey[i], joinForeignKey[i])
                }
            })
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
            whereIn: [],
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
                    rel.referenceKey.map((cn) => `${colTablePath}.${cn}`),
                    rel.foreignKey.map((cn) => `${this.tablePath}.${cn}`),
                ])

                queryConditions.select.push(`${colPath} as ${colPath}`)
            }
        }

        // Group primary keys into arrays of values for the same key.
        const primaryKeys = {}
        Object.keys(this.primaryKeyData[0]).forEach((key) => {
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

    /**
     * Just use all linked property columns.
     */
    _findInputArgColumns() {
        const linkProperties = this.linkProperties
        const inputArgColPaths = []
        const colPathToFunctionInputArg = {}
        for (let property in this.linkProperties) {
            const colPath = linkProperties[property]
            inputArgColPaths.push(colPath)
            colPathToFunctionInputArg[colPath] = property
        }

        this.inputArgColPaths = inputArgColPaths
        this.colPathToFunctionInputArg = colPathToFunctionInputArg

        // Get the live object property keys associated with each input column.
        const reverseLinkProperties = this.reverseLinkProperties
        this.inputPropertyKeys = this.inputArgColPaths.map(
            (colPath) => reverseLinkProperties[colPath]
        )
    }
}

export default ResolveRecordsService
