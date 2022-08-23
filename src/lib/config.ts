import toml from '@ltd/j-toml'
import fs from 'fs'
import constants from './constants'
import { ConfigError } from './errors'
import { noop, toMap } from './utils/formatters'
import logger from './logger'
import { tablesMeta, pullTableMeta, getRel } from './db/tablesMeta'
import {
    ProjectConfig, 
    LiveObjectsConfig, 
    LiveObjectConfig,
    TablesConfig,
    TableConfig,
    StringMap,
    TableDataSources,
    LiveObjectLink,
} from './types'

class Config {

    config: ProjectConfig
    
    isValid: boolean

    linkUniqueConstraints: { [key: string]: string[] } = {}

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
        links: StringMap[],
    }} {
        const m = {}
        const objects = this.liveObjects
        for (let configName in objects) {
            const obj = objects[configName]
            m[obj.id] = {
                id: obj.id,
                configName,
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
                        properties: toMap(link.properties),
                    }
                }
            }
        }
        return null
    }

    getTable(schemaName: string, tableName: string): TableConfig {
        const tables = this.tables
        const schema = tables[schemaName]
        if (!schema) return null
        return schema[tableName] || null
    }

    getDataSourcesForTable(schemaName: string, tableName: string): TableDataSources | null {
        const table = this.getTable(schemaName, tableName)
        if (!table) return null

        const dataSources = {}
        for (const columnName in table) {
            const source = table[columnName].source
            if (!source) continue
            
            const { object, property } = source
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

    getLiveObjectTableDataSources(matchliveObjectId: string, tablePath: string): TableDataSources {
        const [schema, table] = tablePath.split('.')
        const dataSourcesInTable = config.getDataSourcesForTable(schema, table) || {}

        // Basically just recreate the map, but filtering out the data sources that 
        // aren't associated with our live object. Additionally, use just the live 
        const tableDataSourcesForThisLiveObject = {}
        // object property as the new key (removing the live object id).
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

                for (const colPath of Object.values(link.properties)) {
                    const [schemaName, tableName, colName] = colPath.split('.')
                    const colTablePath = [schemaName, tableName].join('.')
                    tablePaths.add(colTablePath)
                }
            }
        }

        return Array.from(tablePaths)
    }

    getUniqueConstraintForLink(liveObjectId: string, tablePath: string, useCache: boolean = true): string[] | null {
        const cacheKey = [liveObjectId, tablePath].join(':')
        if (useCache) {
            const result = this.linkUniqueConstraints[cacheKey]
            if (result) return result
        }

        const link = this.getLink(liveObjectId, tablePath)
        if (!link) return null

        const uniqueBy = link.uniqueBy || []
        if (!uniqueBy.length) return null

        const { uniqueColGroups } = tablesMeta[tablePath]
        if (!uniqueColGroups.length) return null

        // Resolve uniqueBy properties to their respective column names.
        const uniqueByColNames = []
        for (const property of uniqueBy) {
            const colPath = link.properties[property]
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
        // Get table metadata for all tables referenced in config file.
        if (!(await this._pullMetaForAllTablesInConfig())) {
            this.isValid = false
            return
        }

        // Ensure each link's uniqueBy array has a matching unique constraint.
        if (!this._getUniqueConstraintsForAllLinks()) {
            this.isValid = false
            return
        }

        this.isValid = true
    }

    watch() {
        fs.watch(constants.PROJECT_CONFIG_PATH, () => {
            logger.info('New config file detected.')
            this.onUpdate()
        })
    }

    async _pullMetaForAllTablesInConfig(): Promise<boolean> {
        try {
            await Promise.all(this.getAllReferencedTablePaths().map(tablePath => pullTableMeta(tablePath)))
        } catch (err) {
            logger.error(`Error pulling metadata for tables in config: ${err}`)
            return false
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

        return true
    }

    _logMissingUniqueConstraint(link: LiveObjectLink) {
        const uniqueByColNames = []
        for (const property of link.uniqueBy) {
            const colPath = link.properties[property]
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
            this.config = toml.parse(
                fs.readFileSync(constants.PROJECT_CONFIG_PATH, 'utf-8')
            ) as unknown as ProjectConfig
        } catch (err) {
            throw new ConfigError(err)
        }
    }
}

const config = new Config()
export default config