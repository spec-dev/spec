import toml from '@ltd/j-toml'
import fs from 'fs'
import constants from './constants'
import { ConfigError } from './errors'
import { noop, toMap } from './utils/formatters'
import logger from './logger'
import { tablesMeta, pullTableMeta, getRel } from './db/tablesMeta'
import { cloneDeep } from 'lodash'
import { isTimestampColType } from './utils/colTypes'
import {
    ProjectConfig, 
    LiveObjectsConfig, 
    LiveObjectConfig,
    TablesConfig,
    TableConfig,
    StringMap,
    StringKeyMap,
    TableDataSources,
    LiveObjectLink,
    TableLink,
    ColumnConfig,
} from './types'
import { tableSubscriber } from './db/subscriber'

class Config {

    config: ProjectConfig
    
    isValid: boolean

    linkUniqueConstraints: { [key: string]: string[] } = {}

    checkTablesTimer: any = null

    fileContents: string = ''

    onUpdate: () => void

    get projectId(): string {
        return this.config.project_id
    }

    get projectName(): string {
        return this.config.project_name
    }

    get liveObjects(): LiveObjectsConfig {
        return this.config.objects || {}
    }

    get tables(): TablesConfig {
        return this.config.tables || {}
    }

    get liveObjectIds(): string[] {
        const ids = []
        const objects = this.liveObjects
        for (let configName in objects) {
            ids.push(objects[configName].id)
        }
        return ids
    }

    get liveObjectsMap(): { [key: string]: {
        id: string
        configName: string
        filterBy: StringKeyMap,
        links: StringMap[],
    }} {
        const m = {}
        const objects = this.liveObjects
        for (let configName in objects) {
            const obj = objects[configName]
            m[obj.id] = {
                configName,
                id: obj.id,
                filterBy: obj.filterBy,
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

            for (const link of obj.links) {
                if (link.table === tablePath) {
                    return {
                        ...link,
                        inputs: toMap(link.inputs),
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
            for (const link of obj.links) {
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

    getExternalTableLinksDependentOnTableForSeed(tablePath: string): TableLink[] {
        const depTableLinks = []
        const objects = this.liveObjects
        for (const configName in objects) {
            const obj = objects[configName]

            for (const link of obj.links) {
                if (link.table === tablePath) continue

                let allSeedColsOnTable = true
                for (const seedProperty of link.seedWith) {
                    const seedColPath = link.inputs[seedProperty]
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

    getDataSourcesForTable(schemaName: string, tableName: string): TableDataSources | null {
        const table = this.getTable(schemaName, tableName)
        if (!table) return null

        const dataSources = {}
        for (const columnName in table) {
            const dataSource = table[columnName]
            const { object, property } = this._parseDataSourceForColumn(dataSource)
            if (!object || !property) {
                logger.error(`Invalid data source for ${tableName}.${columnName}:`, dataSource)
                continue
            }
            
            const liveObject = this.getLiveObject(object)
            if (!liveObject) continue

            const dataSourceKey = `${liveObject.id}:${property}`
            if (!dataSources.hasOwnProperty(dataSourceKey)) {
                dataSources[dataSourceKey] = []
            }
            dataSources[dataSourceKey].push({ columnName })
        }

        return dataSources
    }

    // TODO: Clean this up with recursion.
    _parseDataSourceForColumn(colDataSource: ColumnConfig | string): StringKeyMap {
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

            for (const link of obj.links) {
                tablePaths.add(link.table)

                for (const colPath of Object.values(link.inputs)) {
                    const [schemaName, tableName, colName] = colPath.split('.')
                    const colTablePath = [schemaName, tableName].join('.')
                    tablePaths.add(colTablePath)
                }
            }
        }

        return Array.from(tablePaths)
    }

    getAllReferencedTablePathsTrackingRecordUpdates(): string[] {
        return this.getAllReferencedTablePaths().filter(tablePath => {
            const meta = tablesMeta[tablePath]
            if (!meta) return false

            const updatedAtColType = meta.colTypes[constants.TABLE_SUB_UPDATED_AT_COL_NAME]
            if (!updatedAtColType) return false

            return isTimestampColType(updatedAtColType)
        })
    }

    getUniqueConstraintForLink(liveObjectId: string, tablePath: string, useCache: boolean = true): string[] | null {
        const cacheKey = [liveObjectId, tablePath].join(':')
        if (useCache) {
            const result = this.linkUniqueConstraints[cacheKey]
            if (result) return result
        }

        const link = this.getLink(liveObjectId, tablePath)
        if (!link) return null

        const uniqueBy = link.uniqueBy || Object.keys(link.inputs) || []
        if (!uniqueBy.length) return null

        const { uniqueColGroups } = tablesMeta[tablePath]
        if (!uniqueColGroups.length) return null

        // Resolve uniqueBy properties to their respective column names.
        const uniqueByColNames = []
        for (const property of uniqueBy) {
            const colPath = link.inputs[property]
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
        return uniqueColGroups.find(colGroup => [...colGroup].sort().join(':') === uniqueByColNamesId) || null
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
        // Validate table schema / meta / constraints.
        if (!(await this._checkTableStructures())) {
            this.isValid = false
            return
        }

        this.isValid = true
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

                const prevPkColNames = prevTableMeta.primaryKey.map(pk => pk.name).sort().join(',')
                const currentPkColNames = currentTableMeta.primaryKey.map(pk => pk.name).sort().join(',')

                if (currentPkColNames !== prevPkColNames) {
                    tablePathsWherePrimaryKeysChanged.push(tablePath)
                }
            }
            if (!tablePathsWherePrimaryKeysChanged.length) return

            // For tables where primary keys have changed, 
            // update their table sub triggers.
            tableSubscriber.upsertTableSubsWithTriggers(
                tablePathsWherePrimaryKeysChanged
            )
        }, constants.ANALYZE_TABLES_INTERVAL)
    }

    async _checkTableStructures(): Promise<boolean> {
        // Get table metadata for all tables referenced in config file.
        if (!(await this._pullMetaForAllTablesInConfig())) {
            return false
        }

        // Ensure each link's uniqueBy array has a matching unique constraint.
        if (!this._getUniqueConstraintsForAllLinks()) {
            return false
        }

        return true
    }

    async _pullMetaForAllTablesInConfig(): Promise<boolean> {
        const priorTablePaths = Object.keys(tablesMeta)
        const allTablePathsReferencedInConfig = this.getAllReferencedTablePaths()

        try {
            await Promise.all(allTablePathsReferencedInConfig.map(tablePath => pullTableMeta(tablePath)))
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

    _getUniqueConstraintsForAllLinks(): boolean {
        const linkUniqueConstraints = {}
        const objects = this.liveObjects
        
        for (const configName in objects) {
            const obj = objects[configName]
            for (const link of obj.links) {
                const uniqueConstraint = this.getUniqueConstraintForLink(obj.id, link.table, false)
                if (!uniqueConstraint) {
                    this._logMissingUniqueConstraint(link)
                    return false
                }
                const key = [obj.id, link.table].join(':')
                linkUniqueConstraints[key] = uniqueConstraint
            }
        }

        this.linkUniqueConstraints = linkUniqueConstraints
        return true
    }

    _logMissingUniqueConstraint(link: LiveObjectLink) {
        const uniqueByColNames = []
        const uniqueByProperties = link.uniqueBy || Object.keys(toMap(link.inputs))
        for (const property of uniqueByProperties) {
            const colPath = link.inputs[property]
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
        logger.error(`No unique constraint exists on table "${link.table}" for column(s): ${uniqueByColNames}`)
    }

    _ensureFileExists() {
        if (!fs.existsSync(constants.PROJECT_CONFIG_PATH)) {
            throw new ConfigError(`Config file does not exist at ${constants.PROJECT_CONFIG_PATH}.`)
        }
    }

    _readAndParseFile() {
        try {
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