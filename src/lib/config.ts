import toml from '@ltd/j-toml'
import fs from 'fs'
import constants from './constants'
import { ConfigError } from './errors'
import { noop } from './utils/formatters'
import logger from './logger'
import {
    ProjectConfig, 
    LiveObjectsConfig, 
    LiveObjectConfig,
    TablesConfig,
    TableConfig,
    StringMap,
    TableDataSources,
} from './types'

class Config {

    config: ProjectConfig
    
    isValid: boolean

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

    getLinkProperties(liveObjectId: string, tablePath: string): StringMap | null {
        const objects = this.liveObjects
        for (const configName in objects) {
            const obj = objects[configName]
            if (obj.id !== liveObjectId) continue

            for (const link of obj.links) {
                if (link.table === tablePath) {
                    return link.properties
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

    load() {
        try {
            this._ensureFileExists()
            this._readAndParseFile()
        } catch (err) {
            logger.error(err.message)
            this.isValid = false
        }
    }

    validate() {
        // TODO: All the checks...

        this.isValid = true
    }

    watch() {
        fs.watch(constants.PROJECT_CONFIG_PATH, () => {
            logger.info('New config file detected.')
            this.onUpdate()
        })
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