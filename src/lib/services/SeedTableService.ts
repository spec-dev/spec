import logger from '../logger'
import { SeedSpec, LiveObject, EdgeFunction, LiveObjectFunctionRole, StringKeyMap, TableDataSources } from '../types'
import { reverseMap } from '../utils/formatters'
import { areColumnsEmpty } from '../db/ops'
import { callSpecFunction } from '../utils/requests'
import config from '../config'
import { db } from '../db'
import { QueryError } from '../errors'

class SeedTableService {

    seedSpec: SeedSpec

    liveObject: LiveObject

    seedFunction: EdgeFunction | null

    tableDataSources: TableDataSources

    constructor(seedSpec: SeedSpec, liveObject: LiveObject) {
        this.seedSpec = seedSpec
        this.liveObject = liveObject
        this.tableDataSources = this._getLiveObjectTableDataSources()
        this.seedFunction = null
    }

    async perform() {
        logger.info(`Seeding ${this.seedSpec.tablePath}...`)

        // Find seed function to use.
        this._findSeedFunction()
        if (!this.seedFunction) throw 'Live object doesn\'t have an associated seed function.'

        // Find the required args that I need to call this function, and then find their corresponding columns.
        const requiredArgColPaths = this._getRequiredArgColumns()
        let isCrossTableLink = false
        const linkPropertyTableColumns = {}
        for (const colPath of requiredArgColPaths) {
            const [schemaName, tableName, colName] = colPath.split('.')
            const tablePath = [schemaName, tableName].join('.')
            if (tablePath !== this.seedSpec.tablePath) {
                isCrossTableLink = true
            }
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
            if (isCrossTableLink) {
                logger.info('Can\'t seed a cross-table relationship from scratch.')
                return
            }
            if (!this.seedSpec.seedIfEmpty) {
                logger.info(`Table not configured to seed when seedIfEmpty isn't truthy: ${this.seedSpec}.`)
                return
            }
            await this._seedFromScratch()
        } else {
            await this._seedWithInputCols()
        }
    }

    async _seedFromScratch() {
        const { data: liveObjectsData, error } = await callSpecFunction(this.seedFunction.name, [])
        if (error) throw error
        if (!liveObjectsData.length) return

        const records = []
        for (const liveObjectData of liveObjectsData) {
            const record = {}
            for (const property in liveObjectData) {
                const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
                const value = liveObjectData[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    record[columnName] = value
                }
            }
            records.push(record)
        }

        try {
            await db.from(this.seedSpec.tablePath).insert(records)
        } catch (err) {
            const [schemaName, tableName] = this.seedSpec.tablePath
            throw new QueryError('insert', schemaName, tableName, err)
        }
    }

    async _seedWithInputCols() {
        // Get records where all required input columns have a value.

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

    _getRequiredArgColumns(): string[] {
        const { argsMap, args } = this.seedFunction
        const reverseArgsMap = reverseMap(argsMap)

        const requiredArgColPaths = []
        for (let inputKey in args) {
            const propertyKey = reverseArgsMap[inputKey] || inputKey
            const isRequiredInput = args[inputKey]

            if (isRequiredInput) {
                requiredArgColPaths.push(this.seedSpec.linkProperties[propertyKey])
            }
        }
        return requiredArgColPaths
    }

    _getSeedTableLinkColNames(): string[] {
        const colNames = []
        for (const propertyKey in this.seedSpec.linkProperties) {
            const [schemaName, tableName, colName] = this.seedSpec.linkProperties[propertyKey].split('.')
            const tablePath = [schemaName, tableName].join('.')
            if (tablePath === this.seedSpec.tablePath) {
                colNames.push(colName)
            }
        }
        return colNames
    }

    _getLiveObjectTableDataSources(): TableDataSources {
        const [schema, table] = this.seedSpec.tablePath.split('.')
        const allTableDataSources = config.getDataSourcesForTable(schema, table) || {}
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