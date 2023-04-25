import toml from '@ltd/j-toml'
import fs from 'fs'
import { constants } from './constants'
import { ConfigError } from './errors'
import { noop, toMap, fromNamespacedVersion, unique } from './utils/formatters'
import { isNonEmptyString, couldBeColPath, couldBeNumber } from './utils/validators'
import logger from './logger'
import { tablesMeta, pullTableMeta, getRel } from './db/tablesMeta'
import { cloneDeep } from 'lodash'
import { isTimestampColType } from './utils/colTypes'
import {
    ProjectConfig,
    LiveObjectsConfig,
    LiveObjectConfig,
    TablesConfig,
    DefaultsConfig,
    TableConfig,
    StringMap,
    StringKeyMap,
    TableDataSources,
    LiveObjectLink,
    TableLink,
    ColumnConfig,
    Filter,
    ColumnDefaultsConfig,
    ColumnDefaultsSetOn,
    EnrichedLink,
    FilterGroup,
    FilterOp,
} from './types'
import {
    filterOps,
    multiValueFilterOps,
    numericFilterOps,
    columnFilterOps,
    isColOperatorOp,
} from './utils/filters'
import { tableSubscriber } from './db/subscriber'

class Config {
    config: ProjectConfig

    isValid: boolean

    enrichedLinks: { [key: string]: EnrichedLink } = {}

    checkTablesTimer: any = null

    fileContents: string = ''

    onUpdate: () => void

    get liveObjects(): LiveObjectsConfig {
        return this.config.objects || {}
    }

    get tables(): TablesConfig {
        return this.config.tables || {}
    }

    get defaults(): DefaultsConfig {
        return this.config.defaults || {}
    }

    get liveObjectIds(): string[] {
        const ids = []
        const objects = this.liveObjects
        for (let configName in objects) {
            objects[configName].id && ids.push(objects[configName].id)
        }
        return ids
    }

    get liveObjectsMap(): {
        [key: string]: {
            id: string
            configName: string
            links: StringMap[]
        }
    } {
        const m = {}
        const objects = this.liveObjects
        for (let configName in objects) {
            const obj = objects[configName]
            m[obj.id] = {
                configName,
                id: obj.id,
                links: obj.links,
            }
        }
        return m
    }

    constructor(onUpdate?: () => void) {
        this.isValid = true
        this.onUpdate = onUpdate || noop
    }

    getLiveObject(configName: string): LiveObjectConfig | null {
        return this.liveObjects[configName] || null
    }

    getEnrichedLink(liveObjectId: string, tablePath: string): EnrichedLink | null {
        return this.enrichedLinks[[liveObjectId, tablePath].join(':')] || null
    }

    getLinksForTable(tablePath: string): TableLink[] {
        const tableLinks = []
        const objects = this.liveObjects
        for (const configName in objects) {
            const obj = objects[configName]
            for (const link of obj.links || []) {
                if (link.table === tablePath) {
                    tableLinks.push({
                        liveObjectId: obj.id,
                        link,
                        enrichedLink: this.getEnrichedLink(obj.id, tablePath),
                    })
                }
            }
        }
        return tableLinks
    }

    getTableOrder(matchTablePath: string): number | null {
        const tables = this.tables
        let i = 0
        for (const schema in tables) {
            for (const tableName in tables[schema]) {
                const tablePath = [schema, tableName].join('.')
                if (tablePath === matchTablePath) return i
                i++
            }
        }
        return null
    }

    getDefaultColumnValuesForTable(tablePath: string): { [key: string]: ColumnDefaultsConfig } {
        const [schemaName, tableName] = tablePath.split('.')
        const defaults = this.defaults
        const schema = defaults[schemaName]
        if (!schema) return {}
        let table = schema[tableName]
        if (!table) return {}
        table = toMap(table)

        const acceptedSetOnValues = [ColumnDefaultsSetOn.Insert, ColumnDefaultsSetOn.Update]

        const defaultColValues = {}
        for (const colName in table) {
            let colDefaults = table[colName]

            if (typeof colDefaults === 'object') {
                if (colDefaults === null || Array.isArray(colDefaults)) continue
                try {
                    colDefaults = toMap(colDefaults) as ColumnDefaultsConfig
                } catch (err) {
                    logger.warn(`Tried to convert colDefaults into map but failed`, colDefaults)
                    continue
                }
                if (!colDefaults.hasOwnProperty('value')) continue
                const value = colDefaults.value

                let setOn = (colDefaults.setOn || []).filter((v) => acceptedSetOnValues.includes(v))
                setOn = setOn.length ? setOn : [ColumnDefaultsSetOn.Insert]

                defaultColValues[colName] = { value, setOn }
                continue
            }

            defaultColValues[colName] = {
                value: colDefaults,
                setOn: [ColumnDefaultsSetOn.Insert],
            }
        }

        return defaultColValues
    }

    getExternalTableLinksDependentOnTableForSeed(tablePath: string): TableLink[] {
        const depTableLinks = []
        const objects = this.liveObjects
        for (const configName in objects) {
            const obj = objects[configName]

            for (const link of obj.links || []) {
                if (link.table === tablePath) continue

                const uniqueFilterColPaths = new Set<string>()

                for (let filterGroup of link.filterBy || []) {
                    filterGroup = toMap(filterGroup)

                    for (const filter of Object.values(filterGroup)) {
                        const colPath = filter.column
                        if (!colPath) continue
                        const [schemaName, tableName, _] = colPath.split('.')
                        const colTablePath = [schemaName, tableName].join('.')

                        // Found a link using this tablePath as a column filter.
                        if (colTablePath === tablePath) {
                            uniqueFilterColPaths.add(colPath)
                        }
                    }
                }

                const filterColPaths = Array.from(uniqueFilterColPaths)
                if (!filterColPaths.length) continue

                depTableLinks.push({
                    liveObjectId: obj.id,
                    link,
                    enrichedLink: this.getEnrichedLink(obj.id, link.table),
                    filterColPaths,
                })
            }
        }
        return depTableLinks
    }

    getTable(schemaName: string, tableName: string): TableConfig {
        const tables = this.tables
        const schema = tables[schemaName]
        if (!schema) return null
        const table = schema[tableName]
        return table ? toMap(table) : null
    }

    getDataSourcesForTable(
        schemaName: string,
        tableName: string,
        returnNullIfError?: boolean
    ): TableDataSources | null {
        const table = this.getTable(schemaName, tableName)
        if (!table) {
            logger.error(`No table exists in config for path "${schemaName}.${tableName}".`)
            return null
        }

        const dataSources = {}
        for (const columnName in table) {
            const dataSource = table[columnName]
            const { object, property } = this.parseDataSourceForColumn(dataSource)

            if (!object || !property) {
                logger.error(`Invalid data source for ${tableName}.${columnName}:`, dataSource)
                if (returnNullIfError) {
                    return null
                }
                continue
            }

            const liveObject = this.getLiveObject(object)
            if (!liveObject) {
                logger.error(
                    `${tableName}.${columnName} references a live object not found in the config "${object}".`
                )
                if (returnNullIfError) {
                    return null
                }
                continue
            }

            const dataSourceKey = `${liveObject.id}:${property}`
            if (!dataSources.hasOwnProperty(dataSourceKey)) {
                dataSources[dataSourceKey] = []
            }
            dataSources[dataSourceKey].push({ columnName })
        }

        return dataSources
    }

    // TODO: Clean this up with recursion.
    parseDataSourceForColumn(colDataSource: ColumnConfig | string): StringKeyMap {
        if (typeof colDataSource === 'string') {
            const propertyPath = colDataSource.split('.')
            return propertyPath.length === 2
                ? { object: propertyPath[0], property: propertyPath[1] }
                : {}
        }

        if (typeof colDataSource === 'object') {
            const source = colDataSource.source
            if (typeof source === 'string') {
                const propertyPath = source.split('.')
                return propertyPath.length === 2
                    ? { object: propertyPath[0], property: propertyPath[1] }
                    : {}
            }
            return typeof source === 'object' ? source : {}
        }

        return {}
    }

    getLiveObjectTableDataSources(matchliveObjectId: string, tablePath: string): TableDataSources {
        const [schema, table] = tablePath.split('.')
        const dataSourcesInTable = config.getDataSourcesForTable(schema, table) || {}

        // Basically just recreate the map, but filtering out the data sources that
        // aren't associated with our live object. Additionally, use just the live
        // object property as the new key (removing the live object id).
        const tableDataSourcesForThisLiveObject = {}
        for (let key in dataSourcesInTable) {
            const [liveObjectId, property] = key.split(':')
            if (liveObjectId !== matchliveObjectId) continue
            tableDataSourcesForThisLiveObject[property] = dataSourcesInTable[key]
        }
        return tableDataSourcesForThisLiveObject
    }

    getAllReferencedTablePaths(): string[] {
        const tablePaths = new Set<string>()
        const objects = this.liveObjects

        for (const configName in objects) {
            const obj = objects[configName]

            for (const link of obj.links || []) {
                tablePaths.add(link.table)

                for (let filterGroup of link.filterBy || []) {
                    filterGroup = toMap(filterGroup)

                    for (const filter of Object.values(filterGroup)) {
                        const colPath = filter.column
                        if (!colPath) continue
                        const [schemaName, tableName, _] = colPath.split('.')
                        const colTablePath = [schemaName, tableName].join('.')
                        tablePaths.add(colTablePath)
                    }
                }
            }
        }

        return Array.from(tablePaths)
    }

    getAllReferencedTablePathsTrackingRecordUpdates(): string[] {
        return this.getAllReferencedTablePaths().filter((tablePath) => {
            const meta = tablesMeta[tablePath]
            if (!meta) return false

            const updatedAtColType = meta.colTypes[constants.TABLE_SUB_UPDATED_AT_COL_NAME]
            if (!updatedAtColType) return false

            return isTimestampColType(updatedAtColType)
        })
    }

    load(): boolean {
        try {
            this._ensureFileExists()
            this._readAndParseFile()
        } catch (err) {
            logger.error(err.message)
            this.isValid = false
            return false
        }
        return true
    }

    async validate() {
        let valid = true

        try {
            if (!this._validateObjectsSection()) {
                valid = false
            }
            if (!this._validateTablesSection()) {
                valid = false
            }
            if (!(await this._checkTableStructures())) {
                valid = false
            }
        } catch (err) {
            logger.error(`Unexpected error occurred while validating config: ${err}`)
            valid = false
        }

        this.isValid = valid
    }

    watch() {
        // Watch config file for any changes.
        fs.watch(constants.PROJECT_CONFIG_PATH, () => {
            setTimeout(() => {
                // Ensure the file contents actually changed.
                const newContents = fs
                    .readFileSync(constants.PROJECT_CONFIG_PATH, 'utf-8')
                    .toString()
                if (newContents === this.fileContents) return
                this.fileContents = newContents
                logger.info('New config file detected.')
                this.onUpdate()
            }, 50)
        })

        // Refresh table metadata on an interval to catch
        // any table schema changes as best as possible.
        this._checkTablesOnInterval()
    }

    _checkTablesOnInterval() {
        if (this.checkTablesTimer !== null) return

        this.checkTablesTimer = setInterval(async () => {
            // Clone tablesMeta.
            const prevTablesMeta = cloneDeep(tablesMeta)

            // Update tables meta and uniqueBy constraints cache.
            if (!(await this._checkTableStructures())) return

            // Check each table to see if primary keys have changed.
            const tablePathsWherePrimaryKeysChanged = []
            for (const tablePath in tablesMeta) {
                const currentTableMeta = tablesMeta[tablePath]
                const prevTableMeta = prevTablesMeta[tablePath]
                if (!prevTableMeta) continue

                const prevPkColNames = prevTableMeta.primaryKey
                    .map((pk) => pk.name)
                    .sort()
                    .join(',')
                const currentPkColNames = currentTableMeta.primaryKey
                    .map((pk) => pk.name)
                    .sort()
                    .join(',')

                if (currentPkColNames !== prevPkColNames) {
                    tablePathsWherePrimaryKeysChanged.push(tablePath)
                }
            }
            if (!tablePathsWherePrimaryKeysChanged.length) return

            // For tables where primary keys have changed,
            // update their table sub triggers.
            tableSubscriber.upsertTableSubsWithTriggers(tablePathsWherePrimaryKeysChanged)
        }, constants.ANALYZE_TABLES_INTERVAL)
    }

    async _checkTableStructures(): Promise<boolean> {
        // Get table metadata for all tables referenced in config file.
        if (!(await this._pullMetaForAllTablesInConfig())) {
            return false
        }

        // Use table metadata/constraints to build and cache links.
        if (!this._buildEnrichedLinks()) {
            return false
        }

        return true
    }

    _buildEnrichedLinks(): boolean {
        const enrichedLinks = {}
        const objects = this.liveObjects
        let isValid = true

        for (const configName in objects) {
            const obj = objects[configName]

            for (const link of obj.links || []) {
                const key = [obj.id, link.table].join(':')

                const enrichedLink = this._enrichLink(link, obj.id)
                if (!enrichedLink) {
                    isValid = false
                    continue
                }

                enrichedLinks[key] = enrichedLink
            }
        }
        if (!isValid) return false

        this.enrichedLinks = enrichedLinks
        return true
    }

    _enrichLink(link: LiveObjectLink, liveObjectId: string): EnrichedLink | null {
        const tablePath = link.table
        const meta = tablesMeta[tablePath]
        const primaryKeyColNames = meta?.primaryKey?.map((col) => col.name) || []

        let uniqueColGroups = meta.uniqueColGroups || []
        uniqueColGroups = uniqueColGroups.length ? uniqueColGroups : [primaryKeyColNames]
        if (!uniqueColGroups.length) {
            logger.error(`No unique constraints found on table ${tablePath}`)
            return null
        }

        const liveObjectTableDataSources = this.getLiveObjectTableDataSources(
            liveObjectId,
            tablePath
        )
        const uniqueByProperties = link.uniqueBy || []
        const filterBy = link.filterBy || []
        let uniqueByColNames = []
        let propertyColPathMappings = {}

        // REQ: For each property in uniqueByProperties...
        // EITHER, the property is being used as a live column in this table...
        // OR, the property exists in one of the filterBy groups as a column filter...
        // AND the table it's linked to is EITHER the target table OR a foreign table with an actual relationship.

        for (const property of uniqueByProperties) {
            let liveColName
            for (const filterGroup of filterBy) {
                const colPath = (filterGroup[property] || {}).column
                if (!colPath) continue

                const [colSchema, colTable, colName] = colPath.split('.')
                const colTablePath = [colSchema, colTable].join('.')

                if (colTablePath === tablePath) {
                    propertyColPathMappings[property] = colPath
                    liveColName = [colName]
                    break
                }

                const rel = getRel(tablePath, colTablePath)
                if (rel) {
                    propertyColPathMappings[property] = colPath
                    liveColName = rel.foreignKey[rel.referenceKey.indexOf(colName)]
                    break
                }
            }

            const liveColNamesUsingProperty = (liveObjectTableDataSources[property] || []).map(
                (ds) => ds.columnName
            )
            if (liveColNamesUsingProperty.length > 1) {
                logger.error(
                    `A "uniqueBy" property (${property}) shouldn't be mapped to multiple columns in the same table (${tablePath}).`
                )
                return null
            }

            const liveColNameOnTargetTable = liveColNamesUsingProperty[0]
            if (liveColNameOnTargetTable) {
                liveColName = liveColNameOnTargetTable
                propertyColPathMappings[property] =
                    propertyColPathMappings[property] ||
                    [tablePath, liveColNameOnTargetTable].join('.')
            }

            if (liveColName) {
                uniqueByColNames.push(liveColName)
                continue
            }

            logger.error(
                `[${liveObjectId} <> ${tablePath}] Failed to find valid column mappings for\n` +
                    `the "uniqueBy" property: ${property}`
            )
            return null
        }

        // Sort and convert to string to match against.
        uniqueByColNames = unique(uniqueByColNames)
        const uniqueByColNamesId = uniqueByColNames.sort().join(':')

        // Find the matching unique col group (if any).
        let uniqueConstraint = uniqueColGroups.find(
            (colGroup) => [...colGroup].sort().join(':') === uniqueByColNamesId
        )

        // If no matching col group exactly, find one that's even more unique.
        if (!uniqueConstraint) {
            uniqueConstraint = uniqueColGroups.find((colGroup) =>
                uniqueByColNames.every((val) => colGroup.includes(val))
            )
        }
        if (!uniqueConstraint) {
            logger.error(
                `Failed to find a matching unique constraint for the following \n` +
                    `column group on table ${tablePath}: ${uniqueByColNames.join(', ')}`
            )
            return null
        }

        if (filterBy.length && !this._validateFilterByColPaths(filterBy, tablePath)) {
            logger.error(`[${liveObjectId} <> ${tablePath}] Invalid filters.`)
            return null
        }

        return {
            liveObjectId,
            tablePath,
            uniqueByProperties,
            uniqueConstraint,
            linkOn: propertyColPathMappings,
            filterBy,
        }
    }

    _validateFilterByColPaths(filterBy: FilterGroup[], targetTablePath: string): boolean {
        // Aggregate all table paths referenced across all column filters.
        const colTablePathsSet = new Set<string>()
        for (let filterGroup of filterBy) {
            filterGroup = toMap(filterGroup)
            for (const filter of Object.values(filterGroup)) {
                const colPath = filter.column
                if (!colPath) continue
                const [colSchema, colTable, _] = colPath.split('.')
                const colTablePath = [colSchema, colTable].join('.')
                colTablePathsSet.add(colTablePath)
            }
        }

        const colTablePaths = Array.from(colTablePathsSet)
        const foreignTablePaths = colTablePaths.filter((tablePath) => tablePath !== targetTablePath)

        // Only allow 1 foreign table to be referenced for now.
        if (foreignTablePaths.length > 1) {
            logger.error(`Only 1 foreign table can be used within "filterBy".`)
            return false
        }

        // If the target table path is referenced in column filters...
        if (colTablePathsSet.has(targetTablePath)) {
            // Ensure a relationship exists between the target table and any referenced foreign table paths.
            for (const foreignTablePath of foreignTablePaths) {
                if (!getRel(targetTablePath, foreignTablePath)) {
                    logger.error(
                        `An explicit relationship must exist between ${targetTablePath} and ${foreignTablePath}\n` +
                            'in order to use both as column filters in the same link.'
                    )
                    return false
                }
            }
        }

        return true
    }

    _validateObjectsSection(): boolean {
        const objects = this.liveObjects
        let isValid = true

        for (let configName in objects) {
            const obj = objects[configName]

            // Ensure object has id.
            if (obj.id) {
                const { nsp, name, version } = fromNamespacedVersion(obj.id)

                // Ensure object has valid id version structure.
                if (!nsp || !name || !version) {
                    logger.error(
                        `Live object "${configName}" has malformed id: ${obj.id}.\nMake sure the id is in "<namespace>.<name>@<version>" format.`
                    )
                    isValid = false
                }
            } else {
                logger.error(`Live object "${configName}" is missing id.`)
                isValid = false
            }

            // Ensure object has links.
            if (!obj.links?.length) {
                logger.error(`Live object "${configName}" has no links.`)
                isValid = false
            }

            // Validate each link.
            for (const link of obj.links || []) {
                const tablePath = link.table || ''

                // Ensure table path exists.
                if (tablePath) {
                    const splitTablePath = tablePath.split('.')

                    // Ensure table path is valid.
                    if (splitTablePath.length === 2) {
                        const [schema, table] = splitTablePath

                        // Ensure table is included in config.
                        if (!this.getTable(schema, table)) {
                            logger.error(
                                `Link for live object "${configName}" has invalid "table" attribute: ${tablePath}. \nValue references table not included in config file.`
                            )
                            isValid = false
                        }
                    } else {
                        logger.error(
                            `Link for live object "${configName}" has invalid "table" attribute: ${tablePath}. \nMust be in "<schema>.<table>" format.`
                        )
                        isValid = false
                    }
                } else {
                    logger.error(
                        `Link for live object "${configName}" is missing the "table" attribute.`
                    )
                    isValid = false
                }

                // Validate uniqueBy properties.
                const uniqueBy = link.uniqueBy || []
                if (!uniqueBy.length) {
                    logger.error(
                        `Link for live object "${configName}" is missing or has an empty value for "uniqueBy".`
                    )
                    isValid = false
                }
                for (const property of uniqueBy) {
                    if (!isNonEmptyString(property)) {
                        logger.error(
                            `Link for live object "${configName}" has invalid "uniqueBy" property: ${property}.`
                        )
                        isValid = false
                    }
                }

                // Validate filters.
                const filterBy = link.filterBy || []
                for (const group of filterBy) {
                    let groupHasColOperatorFilter = false
                    let groupHasEqualColumnFilter = false

                    for (const property in group || {}) {
                        const filter = group[property]
                        const isColumnFilter = !!filter.column
                        const isColOperatorFilter = isColumnFilter && isColOperatorOp(filter.op)
                        const isEqualColumnFilter = isColumnFilter && filter.op === FilterOp.EqualTo
                        groupHasColOperatorFilter = groupHasColOperatorFilter || isColOperatorFilter
                        groupHasEqualColumnFilter = groupHasEqualColumnFilter || isEqualColumnFilter

                        if (!this._validateFilter(configName, property, filter)) {
                            isValid = false
                        }
                    }

                    if (groupHasColOperatorFilter && !groupHasEqualColumnFilter) {
                        logger.error(
                            `Filter groups with column operator filters (>, <, etc.) must also contain an =column filter.`
                        )
                        isValid = false
                    }
                }
            }
        }
        return isValid
    }

    _validateFilter(liveObjectConfigName: string, property: string, filter: Filter): boolean {
        const baseErrMessage = `Link for live object "${liveObjectConfigName}" has invalid filter for property ${property}`
        filter = toMap(filter) as Filter

        if (!filter) {
            logger.error(`${baseErrMessage} - filter was empty`)
            return false
        }

        if (!filter.op) {
            logger.error(`${baseErrMessage} - no "op" given`)
            return false
        }

        if (!filterOps.has(filter.op)) {
            logger.error(`${baseErrMessage} - invalid filter "op": ${filter.op}`)
            return false
        }

        const hasValue = filter.hasOwnProperty('value')
        const hasColumn = filter.hasOwnProperty('column')

        if (hasColumn && !couldBeColPath(filter.column)) {
            logger.error(`${baseErrMessage} - invalid column path: ${filter.column}`)
            return false
        }

        if (hasColumn && !columnFilterOps.has(filter.op)) {
            logger.error(`${baseErrMessage} - invalid op for column filter: ${filter.op}`)
            return false
        }

        if (hasValue && multiValueFilterOps.has(filter.op) && !Array.isArray(filter.value)) {
            logger.error(
                `${baseErrMessage} - Invalid value for array-type filter operation (${filter.op}): ${filter.value}`
            )
            return false
        }

        if (hasValue && numericFilterOps.has(filter.op) && !couldBeNumber(filter.value)) {
            logger.error(
                `${baseErrMessage} - Invalid value for numeric-type filter operation (${filter.op}): ${filter.value}`
            )
            return false
        }

        return true
    }

    _validateTablesSection(): boolean {
        for (const schemaName in config.tables) {
            for (const tableName in config.tables[schemaName]) {
                const dataSources = config.getDataSourcesForTable(schemaName, tableName, true)
                // Will be null if there's an error.
                if (dataSources === null) {
                    return false
                }
            }
        }
        return true
    }

    async _pullMetaForAllTablesInConfig(): Promise<boolean> {
        const priorTablePaths = Object.keys(tablesMeta)
        const allTablePathsReferencedInConfig = this.getAllReferencedTablePaths()

        // Pull table metadata for all table paths referenced in the config file.
        try {
            await Promise.all(
                allTablePathsReferencedInConfig.map((tablePath) => pullTableMeta(tablePath))
            )
        } catch (err) {
            logger.error(`Error pulling metadata for tables in config: ${err}`)
            return false
        }

        // Remove any tablesMeta entries that aren't referenced in config anymore.
        const newTablePaths = new Set(allTablePathsReferencedInConfig)
        for (const priorTablePath of priorTablePaths) {
            if (!newTablePaths.has(priorTablePath)) {
                delete tablesMeta[priorTablePath]
                tableSubscriber.deleteTableSub(priorTablePath)
            }
        }

        return true
    }

    _ensureFileExists() {
        if (!fs.existsSync(constants.PROJECT_CONFIG_PATH)) {
            throw new ConfigError(`Config file does not exist at ${constants.PROJECT_CONFIG_PATH}.`)
        }
    }

    _readAndParseFile() {
        try {
            const file = fs.readFileSync(constants.PROJECT_CONFIG_PATH, 'utf-8')
            this.config = toml.parse(file, { bigint: false }) as unknown as ProjectConfig
            this.fileContents = file.toString()
        } catch (err) {
            throw new ConfigError(err)
        }
    }
}

const config = new Config()
export default config
