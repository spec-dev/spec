import logger from '../logger'
import { SeedSpec, LiveObject, EdgeFunction, LiveObjectFunctionRole, StringKeyMap, TableDataSources } from '../types'
import { reverseMap } from '../utils/formatters'
import { areColumnsEmpty } from '../db/ops'
import { callSpecFunction } from '../utils/requests'
import config from '../config'
import { db } from '../db'

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

        // Find me just the required arguments that I need to call this function and then find me those corresponding columns.
        const requiredArgColPaths = this._getRequiredArgColumns()

        const linkPropertyTableColumns = {}
        for (const colPath of requiredArgColPaths) {
            const [schemaName, tableName, colName] = colPath.split('.')
            const tablePath = [schemaName, tableName].join('.')
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
        allRequiredInputColsAreEmpty ? await this._seedFromScratch() : await this._seedWithInputCols()   
    }

    async _seedFromScratch() {
        if (!this.seedSpec.seedIfEmpty) return
        const { data, error } = await callSpecFunction(this.seedFunction.name, [])
        if (error) throw error
        if (!data.length) return

        const uniqueColNames = this._getSeedTableLinkColNames()
        const properties = Object.keys(data[0])
        const records = []
        for (const entry of data) {
            const record = {}
            for (const property of properties) {
                const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
                const value = entry[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    record[columnName] = value
                }
            }
            records.push(record)
        }

        // What's unique needs to be explicit somehow...and that honestly depends on the relationship between the live object
        // and the tables...because what you classicly think would be "unique" could easily be not-unique as soon as you
        // introduce a time dimension...
        try {
            await db.from(this.seedSpec.tablePath)
                .insert(records)
        } catch (err) {
            logger.error(`Error upserting ${this.seedSpec.tablePath}: ${err}`)
        }
    }

    async _seedWithInputCols() {
        // Get records where all required input columns have a value.

    }

    _createUpsertOp(entry: StringKeyMap) {

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