import toml from '@ltd/j-toml'
import fs from 'fs'
import constants from './constants'
import { ConfigError } from './errors'
import { noop, toMap, fromNamespacedVersion, unique } from './utils/formatters'
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
    FilterOp,
    ColumnDefaultsConfig,
    ColumnDefaultsSetOn,
} from './types'
import { tableSubscriber } from './db/subscriber'

class Config {
    config: ProjectConfig

    prevConfig: ProjectConfig

    isValid: boolean

    linkUniqueConstraints: { [key: string]: string[] } = {}

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
            filterBy: StringKeyMap
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
                filterBy: toMap(obj.filterBy || {}),
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

    getLink(liveObjectId: string, tablePath: string): LiveObjectLink {
        const objects = this.liveObjects
        for (const configName in objects) {
            const obj = objects[configName]
            if (obj.id !== liveObjectId) continue

            for (const link of obj.links || []) {
                if (link.table === tablePath) {
                    return {
                        ...link,
                        linkOn: toMap(link.linkOn || {}),
                    }
                }
            }
        }
        return null
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
                    })
                }
            }
        }
        return tableLinks
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

    getSeedColPaths(seedWith: string | string[] | StringMap, linkOn: StringMap): StringMap[] {
        if (!seedWith) return []
        const isString = typeof seedWith === 'string'
        const isArray = Array.isArray(seedWith)

        // String or array of object properties.
        if (isString || isArray) {
            const seedProperties = (isArray ? seedWith : [seedWith]) as any[]
            linkOn = toMap(linkOn || {})

            let seedColPaths = []
            let newEntry = {}
            for (const val of seedProperties) {
                if (typeof val === 'object') {
                    if (Object.keys(newEntry).length) {
                        seedColPaths.push(newEntry)
                        newEntry = {}
                    }
                    seedColPaths.push(toMap(val))
                } else if (linkOn.hasOwnProperty(val)) {
                    newEntry[val] = linkOn[val]
                } else {
                    if (Object.keys(newEntry).length) {
                        seedColPaths.push(newEntry)
                        newEntry = {}
                    }
                }
            }
            if (Object.keys(newEntry).length) {
                seedColPaths.push(newEntry)
            }
            return seedColPaths
        }

        // Map of property:colPath
        if (typeof seedWith === 'object') {
            return [toMap(seedWith || {})]
        }

        return []
    }

    getExternalTableLinksDependentOnTableForSeed(tablePath: string): TableLink[] {
        const depTableLinks = []
        const objects = this.liveObjects
        for (const configName in objects) {
            const obj = objects[configName]

            for (const link of obj.links || []) {
                if (link.table === tablePath) continue

                const seedColPaths = this.getSeedColPaths(link.seedWith, link.linkOn)
                if (!seedColPaths.length) continue

                const uniqueSeedColPaths = unique(
                    seedColPaths.map((entry) => Object.values(entry)).flat()
                )
                let allSeedColsOnTable = true
                for (const seedColPath of uniqueSeedColPaths) {
                    const [seedColSchema, seedColTable, _] = seedColPath.split('.')
                    const seedColTablePath = [seedColSchema, seedColTable].join('.')
                    if (seedColTablePath !== tablePath) {
                        allSeedColsOnTable = false
                        break
                    }
                }

                if (allSeedColsOnTable) {
                    depTableLinks.push({
                        liveObjectId: obj.id,
                        link,
                        seedColPaths: uniqueSeedColPaths,
                    })
                }
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
                const linkOn = toMap(link.linkOn || {})

                for (const colPath of Object.values(linkOn)) {
                    const [schemaName, tableName, _] = colPath.split('.')
                    const colTablePath = [schemaName, tableName].join('.')
                    tablePaths.add(colTablePath)
                }

                const uniqueSeedColPaths = unique(
                    this.getSeedColPaths(link.seedWith, linkOn)
                        .map((entry) => Object.values(entry))
                        .flat()
                )
                for (const colPath of uniqueSeedColPaths) {
                    const [schemaName, tableName, _] = colPath.split('.')
                    const colTablePath = [schemaName, tableName].join('.')
                    tablePaths.add(colTablePath)
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

    getUniqueConstraintForLink(
        liveObjectId: string,
        tablePath: string,
        useCache: boolean = true
    ): string[] | null {
        const cacheKey = [liveObjectId, tablePath].join(':')
        if (useCache) {
            const result = this.linkUniqueConstraints[cacheKey]
            if (result) return result
        }

        const link = this.getLink(liveObjectId, tablePath)
        if (!link) return null

        const uniqueBy = link.uniqueBy || Object.keys(link.linkOn) || []
        if (!uniqueBy.length) return null

        const meta = tablesMeta[tablePath]
        const primaryKeyColNames = meta?.primaryKey?.map((col) => col.name) || []
        let uniqueColGroups = meta.uniqueColGroups || []
        uniqueColGroups = uniqueColGroups.length ? uniqueColGroups : [primaryKeyColNames]
        if (!uniqueColGroups.length) return null

        // Resolve uniqueBy properties to their respective column names.
        const uniqueByColNames = []
        for (const property of uniqueBy) {
            const colPath = link.linkOn[property]
            if (!colPath) return null
            const [colSchema, colTable, colName] = colPath.split('.')
            const colTablePath = [colSchema, colTable].join('.')

            if (colTablePath === link.table) {
                uniqueByColNames.push(colName)
            } else {
                const foreignKeyConstraint = getRel(link.table, colTablePath)
                if (!foreignKeyConstraint) return null
                uniqueByColNames.push(foreignKeyConstraint.foreignKey)
            }
        }

        // Sort and convert to string to match against.
        const uniqueByColNamesId = uniqueByColNames.sort().join(':')

        // Find the matching unique col group (if any).
        let uniqueColGroup = uniqueColGroups.find(
            (colGroup) => [...colGroup].sort().join(':') === uniqueByColNamesId
        )

        // If no matching col group exactly, find one that's even more unique.
        if (!uniqueColGroup) {
            uniqueColGroup = uniqueColGroups.find((colGroup) =>
                uniqueByColNames.every((val) => colGroup.includes(val))
            )
        }

        return uniqueColGroup || null
    }

    categorizeFilters(filters: StringKeyMap): { [key: string]: Filter }[] {
        const staticFilters = {}
        const columnFilters = {}

        for (const key in filters) {
            let value = filters[key]

            // String filter.
            if (typeof value !== 'object') {
                staticFilters[key] = {
                    op: FilterOp.EqualTo,
                    value: value,
                }
                continue
            }

            try {
                value = toMap(value)
            } catch (err) {
                logger.warn(`Tried to convert filter into map but failed`, value)
                continue
            }

            // Non-column, object filter.
            if (value.hasOwnProperty('value')) {
                staticFilters[key] = {
                    op: value.op || FilterOp.EqualTo,
                    value: value.value,
                }
                continue
            }

            // Column filter.
            if (value.hasOwnProperty('column')) {
                if (typeof value.column !== 'string') continue
                const splitColPath = (value.column || '').split('.')
                if (splitColPath.length !== 3) continue

                columnFilters[key] = {
                    op: value.op || FilterOp.EqualTo,
                    column: value.column,
                }
            }
        }
        return [staticFilters, columnFilters]
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
            // Ensure the file contents actually changed.
            const newContents = fs.readFileSync(constants.PROJECT_CONFIG_PATH, 'utf-8').toString()
            if (newContents === this.fileContents) return
            this.fileContents = newContents
            logger.info('New config file detected.')
            this.onUpdate()
        })

        // Refresh table metadata on an interval to catch
        // any table schema changes as best as possible.
        this._checkTablesOnInterval()
    }

    _checkTablesOnInterval() {
        if (this.checkTablesTimer !== null) return

        this.checkTablesTimer = setInterval(async () => {
            // Clone tablesMeta deep.
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

        // Ensure each link's uniqueBy array has a matching unique constraint.
        if (!this._checkUniqueConstraintsForAllLinks()) {
            return false
        }

        return true
    }

    _validateObjectsSection(): boolean {
        const objects = this.liveObjects
        let isValid = true

        for (let configName in objects) {
            const obj = objects[configName]

            // Ensure object has id.
            if (!obj.id) {
                logger.error(`Live object "${configName}" is missing id.`)
                isValid = false
                continue
            }

            // Ensure object has valid id version structure.
            const { nsp, name, version } = fromNamespacedVersion(obj.id)
            if (!nsp || !name || !version) {
                logger.error(
                    `Live object "${configName}" has malformed id: ${obj.id}.\nMake sure the id is in "<namespace>.<name>@<version>" format.`
                )
                isValid = false
            }

            // Ensure object has links.
            if (!obj.links || !obj.links.length) {
                logger.error(`Live object "${configName}" has no links.`)
                isValid = false
                continue
            }

            // Validate each link.
            for (const link of obj.links || []) {
                const tablePath = link.table || ''
                if (!tablePath) {
                    logger.error(
                        `Link for live object "${configName}" is missing the "table" attribute.`
                    )
                    isValid = false
                    continue
                }
                const splitTablePath = tablePath.split('.')

                // Ensure table is valid.
                if (splitTablePath.length !== 2) {
                    logger.error(
                        `Link for live object "${configName}" has invalid "table" attribute: ${tablePath}. \nMust be in "<schema>.<table>" format.`
                    )
                    isValid = false
                    continue
                }
                const [schema, table] = splitTablePath

                // Ensure table is included in config.
                if (!this.getTable(schema, table)) {
                    logger.error(
                        `Link for live object "${configName}" has invalid "table" attribute: ${tablePath}. \nValue references table not included in config file.`
                    )
                    isValid = false
                }

                // Ensure link has linkOn inputs.
                const linkOn = link.linkOn ? toMap(link.linkOn || {}) : {}
                if (!Object.keys(linkOn).length) {
                    logger.error(`Link for live object "${configName}" has no linkOn inputs.`)
                    isValid = false
                }

                // Ensure input column paths are of valid structure.
                for (const key in linkOn) {
                    const colPath = linkOn[key] || ''
                    const splitColPath = colPath.split('.')

                    if (splitColPath.length !== 3) {
                        logger.error(
                            `Link for live object "${configName}" has invalid input property for key "${key}": ${colPath}. \nMust be in "<schema>.<table>.<column>" format.`
                        )
                        isValid = false
                        continue
                    }
                }

                // // Ensure seedWith properties exist (unless seedIfEmpty is true)
                // if (!link.seedWith && !link.seedIfEmpty) {
                //     logger.error(
                //         `Link for live object "${configName}" has no "seedWith" attribute or it is empty.`
                //     )
                //     isValid = false
                //     continue
                // }
            }
        }
        return isValid
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

    _checkUniqueConstraintsForAllLinks(): boolean {
        const linkUniqueConstraints = {}
        const objects = this.liveObjects
        let isValid = true

        for (const configName in objects) {
            const obj = objects[configName]
            for (const link of obj.links || []) {
                const uniqueConstraint = this.getUniqueConstraintForLink(obj.id, link.table, false)
                if (!uniqueConstraint) {
                    this._logMissingUniqueConstraint(link, obj.id)
                    isValid = false
                    continue
                }
                const key = [obj.id, link.table].join(':')
                linkUniqueConstraints[key] = uniqueConstraint
            }
        }
        if (!isValid) return false

        this.linkUniqueConstraints = linkUniqueConstraints
        return true
    }

    _logMissingUniqueConstraint(link: LiveObjectLink, liveObjectId: string) {
        const uniqueByColNames = []
        const linkOn = toMap(link.linkOn || {})
        const uniqueByProperties = link.uniqueBy || Object.keys(linkOn)
        for (const property of uniqueByProperties) {
            const colPath = linkOn[property]
            if (!colPath) return null
            const [colSchema, colTable, colName] = colPath.split('.')
            const colTablePath = [colSchema, colTable].join('.')

            if (colTablePath === link.table) {
                uniqueByColNames.push(colName)
            } else {
                const foreignKeyConstraint = getRel(link.table, colTablePath)
                if (!foreignKeyConstraint) return null
                uniqueByColNames.push(foreignKeyConstraint.foreignKey)
            }
        }
        logger.error(
            `No unique constraint exists on "${link.table}" for column(s): ${uniqueByColNames}\nPlease add a unique contraint on this group of columns in order to make the link between ${link.table} and ${liveObjectId} work.`
        )
    }

    _ensureFileExists() {
        if (!fs.existsSync(constants.PROJECT_CONFIG_PATH)) {
            throw new ConfigError(`Config file does not exist at ${constants.PROJECT_CONFIG_PATH}.`)
        }
    }

    _readAndParseFile() {
        try {
            if (this.config) {
                this.prevConfig = this.config
            }
            const file = fs.readFileSync(constants.PROJECT_CONFIG_PATH, 'utf-8')
            this.config = toml.parse(file) as unknown as ProjectConfig
            this.fileContents = file.toString()
        } catch (err) {
            throw new ConfigError(err)
        }
    }
}

const config = new Config()
export default config
