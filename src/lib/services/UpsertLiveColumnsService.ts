import { LiveColumnQuerySpec, LiveColumn, LiveColumnSeedStatus } from '../types'
import config from '../config'
import { QueryError } from '../errors'
import { mapBy } from '../utils/formatters'
import { db } from '../db'
import { SPEC_SCHEMA_NAME, LIVE_COLUMNS_TABLE_NAME, getLiveColumnsForColPaths, saveLiveColumns } from '../db/spec'

class UpsertLiveColumnsService {

    querySpecs: LiveColumnQuerySpec[] = []

    prevLiveColumns: LiveColumn[] = []

    liveColumnsToUpsert: LiveColumn[] = []

    tablePathsUsingLiveObjectId: { [key: string]: Set<string> } = {}

    async perform() {
        await this._getExistingLiveColumns()
        await this._upsertLiveColumns()
    }

    async _upsertLiveColumns() {
        this._findLiveColumnsToUpsert()

        if (!this.liveColumnsToUpsert.length) return
        try {
            await saveLiveColumns(this.liveColumnsToUpsert)
        } catch (err) {
            throw new QueryError('upsert', SPEC_SCHEMA_NAME, LIVE_COLUMNS_TABLE_NAME, err)
        }
    }

    async _getExistingLiveColumns() {
        this._getQuerySpecs()
        
        // Get all existing live columns for the given array of column paths.
        try {
            this.prevLiveColumns = await getLiveColumnsForColPaths(this.querySpecs.map(qs => qs.columnPath))
        } catch (err) {
            throw new QueryError('select', SPEC_SCHEMA_NAME, LIVE_COLUMNS_TABLE_NAME, err)
        }
    }

    _findLiveColumnsToUpsert() {
        // Compare querySpecs and prevLiveColumns, to find:
        // -----
        // (1) The live columns that don't exist yet.
        // (2) The live columns where the data source has changed.
        const querySpecsToUpsert = []
        const existingLiveColumns = mapBy<LiveColumn>(this.prevLiveColumns, 'columnPath')
        for (const querySpec of this.querySpecs) {
            if (!existingLiveColumns.hasOwnProperty(querySpec.columnPath) || 
                existingLiveColumns[querySpec.columnPath].liveProperty !== querySpec.liveProperty) {
                querySpecsToUpsert.push(querySpec)
            }
        }
        this.liveColumnsToUpsert = querySpecsToUpsert
    }

    _getQuerySpecs() {
        const liveColumnQuerySpecs = []
        // For each schema...
        for (const schemaName in config.tables) {
            // For each table...
            for (const tableName in config.tables[schemaName]) {
                const tablePath = [schemaName, tableName].join('.')

                // Get all data sources used by the table.
                const dataSources = config.getDataSourcesForTable(schemaName, tableName)

                // For each live object property used by the data sources...
                for (const liveProperty in dataSources) {
                    const [liveObjectId, _] = liveProperty.split(':')
                    if (!this.tablePathsUsingLiveObjectId.hasOwnProperty(liveObjectId)) {
                        this.tablePathsUsingLiveObjectId[liveObjectId] = new Set<string>()
                    }
                    if (!this.tablePathsUsingLiveObjectId[liveObjectId].has(tablePath)) {
                        this.tablePathsUsingLiveObjectId[liveObjectId].add(tablePath)
                    }
                    // For each column using this data source property...
                    for (const { columnName } of dataSources[liveProperty]) {
                        const columnPath = [schemaName, tableName, columnName].join('.')
                        liveColumnQuerySpecs.push({
                            columnPath,
                            liveProperty,
                        })
                    }
                }
            }
        }
        this.querySpecs = liveColumnQuerySpecs
    }
}

export default UpsertLiveColumnsService