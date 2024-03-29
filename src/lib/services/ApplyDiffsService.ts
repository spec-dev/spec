import {
    LiveObject,
    StringKeyMap,
    StringMap,
    Op,
    OpType,
    TableDataSources,
    ColumnDefaultsConfig,
    EnrichedLink,
    FilterGroup,
    FilterOp,
} from '../types'
import config from '../config'
import { db } from '../db'
import { unique, getCombinations, toMap } from '../utils/formatters'
import { QueryError } from '../errors'
import RunOpService from './RunOpService'
import { getRel, isColTypeArray, tablesMeta } from '../db/tablesMeta'
import logger from '../logger'
import { SpecEvent } from '@spec.dev/event-client'
import chalk from 'chalk'
import { withDeadlockProtection } from '../utils/db'
import { isDateColType, isTimestampColType } from '../utils/colTypes'
import { executeFilter } from '../utils/filters'
import { isSpecTimestampFilterFormat } from '../utils/date'
import { identPath } from '../utils/formatters'
import { ident } from 'pg-format'
import { invert } from 'lodash'
import { constants } from '../constants'

const valueSep = '__:__'

class ApplyDiffsService {
    liveObjectDiffs: StringKeyMap[]

    enrichedLink: EnrichedLink

    liveObject: LiveObject

    event: SpecEvent

    filteredDiffs: StringKeyMap[]

    ops: Op[] = []

    liveTableColumns: string[]

    defaultColumnValues: { [key: string]: ColumnDefaultsConfig }

    tableDataSources: TableDataSources

    primaryTimestampColumn: string | null

    get linkTablePath(): string {
        return this.enrichedLink.tablePath
    }

    get linkSchemaName(): string {
        return this.linkTablePath.split('.')[0]
    }

    get linkTableName(): string {
        return this.linkTablePath.split('.')[1]
    }

    get linkProperties(): StringMap {
        return this.enrichedLink.linkOn || {}
    }

    get linkFilters(): FilterGroup[] {
        return this.enrichedLink.filterBy
    }

    get tablePrimaryKeys(): string[] {
        const meta = tablesMeta[this.linkTablePath]
        if (!meta) throw `No meta registered for table ${this.linkTablePath}`
        return meta.primaryKey.map((pk) => pk.name)
    }

    get linkTableUniqueConstraint(): string[] {
        return this.enrichedLink.uniqueConstraint || []
    }

    get linkUniqueByProperties(): string[] {
        return this.enrichedLink.uniqueByProperties || []
    }

    get primaryTimestampProperty(): string | null {
        return this.liveObject.config?.primaryTimestampProperty || null
    }

    constructor(
        diffs: StringKeyMap[],
        enrichedLink: EnrichedLink,
        liveObject: LiveObject,
        event: SpecEvent
    ) {
        this.liveObjectDiffs = diffs
        this.filteredDiffs = diffs
        this.enrichedLink = enrichedLink
        this.liveObject = liveObject
        this.event = event
        this.liveTableColumns = Object.keys(
            config.getTable(this.linkSchemaName, this.linkTableName) || {}
        )
        this.defaultColumnValues = config.getDefaultColumnValuesForTable(this.linkTablePath)
        this.tableDataSources = config.getLiveObjectTableDataSources(
            this.liveObject.id,
            this.linkTablePath
        )
        this.primaryTimestampColumn = this.primaryTimestampProperty
            ? (this.tableDataSources[this.primaryTimestampProperty] || [])[0]?.columnName || null
            : null
    }

    async perform() {
        await this.getOps()
        await this.runOps()
    }

    async getOps(): Promise<Op[]> {
        // Ensure table is reliant on this live object.
        if (!Object.keys(this.tableDataSources).length) {
            logger.error(
                `Table ${this.linkTablePath} isn't reliant on Live Object ${this.liveObject.id}.`
            )
            return this.ops
        }

        // Apply filters to diffs when present.
        if (this.linkFilters.length) {
            await this._filterDiffs()
            if (!this.filteredDiffs.length) return this.ops
        }

        // Convert diffs into upsert operations.
        await this._createUpsertOps()

        return this.ops
    }

    async runOps() {
        if (!this.ops.length) return

        const op = async () => {
            await db.transaction(async (tx) => {
                await Promise.all(this.ops.map((op) => new RunOpService(op, tx).perform()))
            })
        }

        await withDeadlockProtection(op)
    }

    async _filterDiffs() {
        // Indexes of diffs that meet all filter criteria.
        const passingDiffIndexes = new Set<number>()

        // Diffs need to pass ONLY ONE of the filter groups.
        for (let filterGroup of this.linkFilters) {
            filterGroup = toMap(filterGroup)
            const {
                matchingRecordsRegistry,
                colOperatorFilters,
                valueFilters,
                lookupColFilterProperties,
            } = await this._resolveFilterGroup(filterGroup)

            const equalsColumnFiltersExist = lookupColFilterProperties.length > 0
            const otherColumnFiltersExist = Object.keys(colOperatorFilters).length > 0

            // Run each diff through each filter in the group.
            for (let i = 0; i < this.liveObjectDiffs.length; i++) {
                const diff = this.liveObjectDiffs[i]
                if (passingDiffIndexes.has(i)) continue

                // Apply value filters first to avoid lookups.
                const passesValueFilters = this._executeValueFilters(
                    filterGroup,
                    valueFilters,
                    diff
                )
                if (!passesValueFilters) continue

                // If no =column filters exist, the diff passes.
                if (!equalsColumnFiltersExist) {
                    passingDiffIndexes.add(i)
                    continue
                }

                // Apply the =column filters by finding the diff's matching records.
                const matchingRecords =
                    matchingRecordsRegistry[
                        lookupColFilterProperties.map((p) => diff[p]).join(':')
                    ] || []
                if (!matchingRecords.length) continue

                // If no other column filters exist (>, >=, <, <=), the diff passes.
                if (!otherColumnFiltersExist) {
                    passingDiffIndexes.add(i)
                    continue
                }

                // Apply column operator filters.
                const passesColumnOperatorFilters = this._executeColumnOperatorFilters(
                    filterGroup,
                    colOperatorFilters,
                    diff,
                    matchingRecords
                )
                if (!passesColumnOperatorFilters) continue

                // Diff passed all filters.
                passingDiffIndexes.add(i)
            }
        }

        this.filteredDiffs = Array.from(passingDiffIndexes).map((i) => this.liveObjectDiffs[i])
    }

    _executeValueFilters(
        filterGroup: FilterGroup,
        valueFilters: FilterGroup,
        diff: StringKeyMap
    ): boolean {
        for (const property in valueFilters) {
            const filter = filterGroup[property]
            const propertyValue = diff[property]

            // Use filter value format to check if date-time type.
            const isDateTimeColType = isSpecTimestampFilterFormat(filter.value)

            const passesFilter = executeFilter(
                propertyValue,
                filter.op,
                filter.value,
                isDateTimeColType
            )
            if (!passesFilter) {
                return false
            }
        }

        return true
    }

    _executeColumnOperatorFilters(
        filterGroup: FilterGroup,
        colOperatorFilters: FilterGroup,
        diff: StringKeyMap,
        matchingRecords: StringKeyMap[]
    ): boolean {
        for (const record of matchingRecords) {
            let recordPassesFilters = true

            // Apply operator column filters (>, >=, <, <=).
            for (const property in colOperatorFilters) {
                const filter = filterGroup[property]
                const propertyValue = diff[property]
                const colValue = record[filter.column]

                // Use column type to check if date-time type.
                const [colSchema, colTable, colName] = filter.column
                const colTablePath = [colSchema, colTable].join('.')
                const colType = tablesMeta[colTablePath].colTypes[colName]
                const isDateTimeColType = isTimestampColType(colType) || isDateColType(colType)

                const passesFilter = executeFilter(
                    propertyValue,
                    filter.op,
                    colValue,
                    isDateTimeColType
                )
                if (!passesFilter) {
                    recordPassesFilters = false
                    break
                }
            }
            if (!recordPassesFilters) continue

            return true
        }
        return false
    }

    async _resolveFilterGroup(filterGroup: FilterGroup): Promise<{
        matchingRecordsRegistry: { [key: string]: StringKeyMap[] }
        colOperatorFilters: FilterGroup
        valueFilters: FilterGroup
        lookupColFilterProperties: string[]
    }> {
        const propertyColMappings = {}
        const lookupColFilterPaths = []
        const lookupColFilterProperties = []
        const colOperatorFilters = {}
        const valueFilters = {}
        const colFilterTables = new Set()
        const selectColumns = []
        const matchingRecordsRegistry = {}

        for (const property in filterGroup) {
            const filter = filterGroup[property]

            if (filter.column) {
                selectColumns.push(`${filter.column} as ${filter.column}`)
                const [colSchema, colTable, _] = filter.column.split('.')
                colFilterTables.add([colSchema, colTable].join('.'))

                if (filter.op === FilterOp.EqualTo) {
                    propertyColMappings[property] = filter.column
                    lookupColFilterPaths.push(filter.column)
                    lookupColFilterProperties.push(property)
                } else {
                    colOperatorFilters[property] = filter
                }
            } else {
                valueFilters[property] = filter
            }
        }
        const colPropertyMappings = invert(propertyColMappings)

        // Return early if no =column filters exist.
        if (!lookupColFilterProperties.length) {
            return {
                matchingRecordsRegistry,
                colOperatorFilters,
                valueFilters,
                lookupColFilterProperties,
            }
        }

        // If the link table path is referenced at all in this filter group, use it as the lookup table.
        // Otherwise, take the first column filter table, which will be foreign (we can make this assumption
        // because only 1 foreign table can be referenced in a link's filters).
        const lookupTablePath = colFilterTables.has(this.linkTablePath)
            ? this.linkTablePath
            : (Array.from(colFilterTables)[0] as string)

        const queryConditions = {
            select: selectColumns,
            join: [],
            whereIn: [],
            where: [],
        }

        for (const colPath of lookupColFilterPaths) {
            const [colSchemaName, colTableName, _] = colPath.split('.')
            const colTablePath = [colSchemaName, colTableName].join('.')

            if (colTablePath !== lookupTablePath) {
                const rel = getRel(lookupTablePath, colTablePath)
                if (!rel) throw `No rel from ${lookupTablePath} -> ${colTablePath}`

                queryConditions.join.push([
                    colTableName,
                    rel.referenceKey.map((cn) => `${colTablePath}.${cn}`),
                    rel.foreignKey.map((cn) => `${lookupTablePath}.${cn}`),
                ])
            }
        }

        // Use an inclusive-where query if more than one column filter exists in this group.
        // Otherwise, a where-in query will be used.
        const useInclusiveWhere = lookupColFilterPaths.length > 1

        for (const diff of this.liveObjectDiffs) {
            // Where-in
            if (!useInclusiveWhere) {
                const property = lookupColFilterProperties[0]
                const diffValue = diff[property]
                if (diffValue === null) continue
                queryConditions.whereIn.push(diffValue)
                continue
            }

            // Inclusive where
            const where = {}
            let ignoreDiff = false
            for (let i = 0; i < lookupColFilterProperties.length; i++) {
                const diffValue = diff[lookupColFilterProperties[i]]
                if (diffValue === null) {
                    ignoreDiff = true
                    break
                }
                where[lookupColFilterPaths[i]] = diffValue
            }
            if (ignoreDiff) continue
            queryConditions.where.push(where)
        }

        // Return early if no where conditions exist.
        if (!queryConditions.whereIn.length && !queryConditions.where.length) {
            return {
                matchingRecordsRegistry,
                colOperatorFilters,
                valueFilters,
                lookupColFilterProperties,
            }
        }

        // Start building query.
        let query = db.from(lookupTablePath)

        // JOIN foreign tables.
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

        query.select(queryConditions.select)

        if (useInclusiveWhere) {
            const conditions = queryConditions.where
            if (conditions.length) {
                for (let i = 0; i < conditions.length; i++) {
                    const andConditions = conditions[i]
                    const andClauses = (builder) => {
                        let j = 0
                        for (const colPath in andConditions) {
                            const property = colPropertyMappings[colPath]
                            const value = andConditions[colPath]
                            const fuzzyMatch =
                                !!property?.match(/address/i) &&
                                constants.MATCH_CASE_INSENSITIVE_ADDRESSES
                            if (j === 0) {
                                fuzzyMatch
                                    ? builder.whereRaw(`${identPath(colPath)} ~* ?`, [value])
                                    : builder.where(colPath, value)
                            } else {
                                fuzzyMatch
                                    ? builder.andWhereRaw(`${identPath(colPath)} ~* ?`, [value])
                                    : builder.andWhere(colPath, value)
                            }
                            j++
                        }
                    }
                    i === 0 ? query.where(andClauses) : query.orWhere(andClauses)
                }
            }
        } else {
            const whereInColValues = unique(queryConditions.whereIn)
            const filterColPath = lookupColFilterPaths[0]
            const filterProperty = lookupColFilterProperties[0]

            // Fuzzy-match on address properties.
            if (
                whereInColValues.length &&
                !!filterProperty.match(/address/i) &&
                constants.MATCH_CASE_INSENSITIVE_ADDRESSES
            ) {
                query.whereRaw(`${identPath(filterColPath)} ~* ?`, [whereInColValues.join('|')])
            } else {
                whereInColValues.length && query.whereIn(filterColPath, whereInColValues)
            }
        }

        let matchingRecords = []
        try {
            matchingRecords = await query
        } catch (err) {
            const [lookupSchemaName, lookupTableName] = lookupTablePath.split('.')
            throw new QueryError('select', lookupSchemaName, lookupTableName, err)
        }

        // Index matching records by their lookup column values.
        for (const record of matchingRecords) {
            const registryKey = lookupColFilterPaths
                .map((colPath, i) => {
                    let value = record[colPath]
                    const property = lookupColFilterProperties[i]
                    // Auto-lowercase addresses.
                    if (
                        value &&
                        !!property.match(/address/i) &&
                        constants.MATCH_CASE_INSENSITIVE_ADDRESSES
                    ) {
                        value = value.toLowerCase()
                    }
                    // Auto-stringify chain ids.
                    else if (value && !!property.match(/(chainId|chain_id|chainid)/i)) {
                        value = value.toString()
                    }
                    return value
                })
                .join(':')
            matchingRecordsRegistry[registryKey] = matchingRecordsRegistry[registryKey] || []
            matchingRecordsRegistry[registryKey].push(record)
        }

        return {
            matchingRecordsRegistry,
            colOperatorFilters,
            valueFilters,
            lookupColFilterProperties,
        }
    }

    async _createUpsertOps() {
        const tablePath = this.linkTablePath
        const tableDataSources = this.tableDataSources

        let diffs = this.filteredDiffs

        const colFilterGroups = []
        for (let filterGroup of this.linkFilters) {
            filterGroup = toMap(filterGroup)
            const colFilterGroup = {}
            for (const property in filterGroup) {
                const filter = filterGroup[property]
                if (
                    filter.column &&
                    filter.op === FilterOp.EqualTo &&
                    !filter.column.startsWith(tablePath)
                ) {
                    colFilterGroup[property] = filter.column
                }
            }
            Object.keys(colFilterGroup).length && colFilterGroups.push(colFilterGroup)
        }

        // Get query conditions for the linked foreign tables with relationships.
        const foreignTableQueryConditions = {}
        const referenceKeyValues = {}
        for (const colFilterGroup of colFilterGroups) {
            // Reset each loop.
            for (const foreignTablePath in foreignTableQueryConditions) {
                foreignTableQueryConditions[foreignTablePath].whereIn = []
                foreignTableQueryConditions[foreignTablePath].whereRaw = []
                foreignTableQueryConditions[foreignTablePath].properties = []
                foreignTableQueryConditions[foreignTablePath].colNames = []
            }

            for (const property in colFilterGroup) {
                const colPath = colFilterGroup[property]
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = `${colSchemaName}.${colTableName}`
                const rel = getRel(tablePath, colTablePath)
                if (!rel) continue

                foreignTableQueryConditions[colTablePath] = foreignTableQueryConditions[
                    colTablePath
                ] || {
                    rel,
                    tablePath: colTablePath,
                    whereIn: [],
                    whereRaw: [],
                    properties: [],
                    colNames: [],
                    propertyGroups: [],
                    colNameGroups: [],
                }

                foreignTableQueryConditions[colTablePath].properties.push(property)
                foreignTableQueryConditions[colTablePath].colNames.push(colName)

                const values = unique(this.liveObjectDiffs.map((diff) => diff[property]).flat())

                if (isColTypeArray(colPath)) {
                    foreignTableQueryConditions[colTablePath].whereRaw.push([
                        `${ident(colName)} && ARRAY[${values.map(() => '?').join(',')}]`,
                        values,
                    ])
                } else if (
                    !!property.match(/address/i) &&
                    constants.MATCH_CASE_INSENSITIVE_ADDRESSES
                ) {
                    foreignTableQueryConditions[colTablePath].whereRaw.push([
                        `${ident(colName)} ~* ?`,
                        [values.join('|')],
                    ])
                } else {
                    foreignTableQueryConditions[colTablePath].whereIn.push([colName, values])
                }
            }

            for (const foreignTablePath in foreignTableQueryConditions) {
                const queryConditions = foreignTableQueryConditions[foreignTablePath]
                if (!queryConditions.properties.length) continue

                foreignTableQueryConditions[foreignTablePath].propertyGroups.push(
                    queryConditions.properties
                )
                foreignTableQueryConditions[foreignTablePath].colNameGroups.push(
                    queryConditions.colNames
                )

                let query = db.from(foreignTablePath)

                for (let i = 0; i < queryConditions.whereIn.length; i++) {
                    const [col, vals] = queryConditions.whereIn[i]
                    query.whereIn(col, vals)
                }

                for (let j = 0; j < queryConditions.whereRaw.length; j++) {
                    const [sql, bindings] = queryConditions.whereRaw[j]
                    query.whereRaw(sql, bindings)
                }

                let records
                try {
                    records = await query
                } catch (err) {
                    const [foreignSchema, foreignTable] = foreignTablePath.split('.')
                    throw new QueryError('select', foreignSchema, foreignTable, err)
                }
                records = records || []
                if (records.length === 0) {
                    return
                }

                referenceKeyValues[foreignTablePath] = referenceKeyValues[foreignTablePath] || {}
                for (const record of records) {
                    const colValues = queryConditions.colNames.map((colName, i) => {
                        let value = record[colName]
                        const property = queryConditions.properties[i]
                        // Auto-lowercase addresses.
                        if (
                            value &&
                            !!property.match(/address/i) &&
                            constants.MATCH_CASE_INSENSITIVE_ADDRESSES
                        ) {
                            value = value.toLowerCase()
                        }
                        // Auto-stringify chain ids.
                        else if (value && !!property.match(/(chainId|chain_id|chainid)/i)) {
                            value = value.toString()
                        }
                        return value
                    })
                    const colValueOptions = getCombinations(colValues)

                    for (const valueOptions of colValueOptions) {
                        const key = valueOptions.join(valueSep)
                        referenceKeyValues[foreignTablePath][key] =
                            referenceKeyValues[foreignTablePath][key] || []
                        referenceKeyValues[foreignTablePath][key].push(
                            queryConditions.rel.referenceKey.map((c) => record[c]).join(valueSep)
                        )
                    }
                }
            }
        }

        // Format record objects to upsert.
        const upsertRecords = []
        for (const diff of diffs) {
            const upsertRecord: StringKeyMap = {}

            // Map properties -> column_names
            for (const property in diff) {
                const colsWithThisPropertyAsDataSource = tableDataSources[property] || []
                const value = diff[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    upsertRecord[columnName] = value
                }
            }

            // If no relationships to resolve, the record data is ready & formatted.
            if (!Object.keys(foreignTableQueryConditions).length) {
                upsertRecords.push(upsertRecord)
                continue
            }

            let ignoreDiff = false
            const groupedForeignKeyValues = []
            const foreignKeyColNames = []
            for (const foreignTablePath in foreignTableQueryConditions) {
                const queryConditions = foreignTableQueryConditions[foreignTablePath]
                const foreignRecordKeyOptions = queryConditions.propertyGroups
                    .map((group) => group.map((k) => diff[k]).join(valueSep))
                    .flat()

                let foreignRecordReferenceKeyValues = null
                for (const key of foreignRecordKeyOptions) {
                    if (referenceKeyValues[foreignTablePath].hasOwnProperty(key)) {
                        foreignRecordReferenceKeyValues = unique(
                            referenceKeyValues[foreignTablePath][key] || []
                        )
                        break
                    }
                }

                // If matching foreign record couldn't be found, ignore this diff.
                if (!foreignRecordReferenceKeyValues) {
                    ignoreDiff = true
                    break
                }

                groupedForeignKeyValues.push(foreignRecordReferenceKeyValues)
                foreignKeyColNames.push(queryConditions.rel.foreignKey)
            }
            if (ignoreDiff) continue

            const uniqueForeignKeyCombinations = getCombinations(groupedForeignKeyValues)

            for (const foreignKeyValues of uniqueForeignKeyCombinations) {
                const record = { ...upsertRecord }
                for (let i = 0; i < foreignKeyValues.length; i++) {
                    const fkColNames = foreignKeyColNames[i]
                    const refKeyColValues = foreignKeyValues[i].split(valueSep)
                    for (let j = 0; j < refKeyColValues.length; j++) {
                        record[fkColNames[j]] = refKeyColValues[j]
                    }
                }
                upsertRecords.push(record)
            }
        }
        if (!upsertRecords.length) return

        const op = async () => {
            try {
                await db.transaction(async (tx) => {
                    const upsertBatchOp = {
                        type: OpType.Insert,
                        schema: this.linkSchemaName,
                        table: this.linkTableName,
                        data: upsertRecords,
                        conflictTargets: this.linkTableUniqueConstraint,
                        liveTableColumns: this.liveTableColumns,
                        primaryTimestampColumn: this.primaryTimestampColumn,
                        defaultColumnValues: this.defaultColumnValues,
                    }

                    const origin = this.event.origin
                    const chainId = origin?.chainId
                    const blockNumber = origin?.blockNumber

                    logger.info(
                        chalk.green(
                            `[${chainId}:${blockNumber}] Upserting ${upsertRecords.length} records in ${this.linkSchemaName}.${this.linkTableName}...`
                        )
                    )
                    await new RunOpService(upsertBatchOp, tx).perform()
                })
            } catch (err) {
                throw new QueryError('upsert', this.linkSchemaName, this.linkTableName, err)
            }
        }

        await withDeadlockProtection(op)
    }
}

export default ApplyDiffsService
