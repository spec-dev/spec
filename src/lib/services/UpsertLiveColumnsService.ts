import { LiveColumnQuerySpec, LiveColumn, StringKeyMap } from '../types'
import config from '../config'
import { QueryError } from '../errors'
import { mapBy } from '../utils/formatters'
import { hash } from '../utils/hash'
import {
    SPEC_SCHEMA_NAME,
    LIVE_COLUMNS_TABLE_NAME,
    getLiveColumnsForColPaths,
    saveLiveColumns,
    getCachedLinks,
    LINKS_TABLE_NAME,
    saveLinks,
} from '../db/spec'
import logger from '../logger'

class UpsertLiveColumnsService {

    querySpecs: LiveColumnQuerySpec[] = []

    prevLiveColumns: LiveColumn[] = []

    liveColumnsToUpsert: LiveColumn[] = []

    tablePathsUsingLiveObjectId: { [key: string]: Set<string> } = {}

    newLiveTablePaths: Set<string> = new Set()

    async perform() {
        await this._upsertLiveColumns()
        await this._upsertLinks()
    }

    async _upsertLiveColumns() {
        await this._getExistingLiveColumns()
        this._findLiveColumnsToUpsert()

        if (!this.liveColumnsToUpsert.length) return
        try {
            await saveLiveColumns(this.liveColumnsToUpsert)
        } catch (err) {
            throw new QueryError('upsert', SPEC_SCHEMA_NAME, LIVE_COLUMNS_TABLE_NAME, err)
        }
    }

    async _upsertLinks() {
        // Get links from config.
        const linksInConfig = this._getLinksInConfig()
        const uniqueLinkProperties = Object.keys(linksInConfig).map(key => {
            const [liveObjectId, tablePath] = key.split(':')
            return { liveObjectId, tablePath }
        })
        if (!uniqueLinkProperties.length) return 

        // Get links from database.
        let cachedLinks = []
        try {
            cachedLinks = await getCachedLinks(uniqueLinkProperties)
        } catch (err) {
            throw new QueryError('select', SPEC_SCHEMA_NAME, LINKS_TABLE_NAME, err)
        }
        const mappedCachedLinks = {}
        for (const cachedLink of cachedLinks) {
            const linkKey = [cachedLink.liveObjectId, cachedLink.tablePath].join(':')
            mappedCachedLinks[linkKey] = {
                uniqueBy: cachedLink.uniqueBy,
                filterBy: cachedLink.filterBy || null,
            }
        }

        // Compare links for diffs.
        const linksToUpsert = []
        for (const linkKey in linksInConfig) {
            const [liveObjectId, tablePath] = linkKey.split(':')
            const mostRecentLinkData = linksInConfig[linkKey]
            const linkRecord = {
                liveObjectId,
                tablePath,
                uniqueBy: mostRecentLinkData.uniqueBy.sort().join(','),
                filterBy: mostRecentLinkData.filterBy.length 
                    ? hash(JSON.stringify(mostRecentLinkData.filterBy))
                    : null
            }
            const prevLinkRecord = mappedCachedLinks[linkKey]

            // Completely new link.
            if (!prevLinkRecord) {
                linksToUpsert.push(linkRecord)
                continue
            }

            // Compare current vs. prev uniqueBy
            if (linkRecord.uniqueBy !== prevLinkRecord.uniqueBy) {
                linksToUpsert.push(linkRecord)
                continue
            }            

            // Compare current vs. prev filterBy
            if (linkRecord.filterBy !== prevLinkRecord.filterBy) {
                linksToUpsert.push(linkRecord)
            }    
        }
        if (!linksToUpsert.length) return

        // Save the links that changed.
        try {
            await saveLinks(linksToUpsert)
        } catch (err) {
            throw new QueryError('upsert', SPEC_SCHEMA_NAME, LINKS_TABLE_NAME, err)
        }

        // Aggregate all live columns by their links.
        const allLiveColumnsByLink = {}
        for (const { columnPath, liveProperty } of this.querySpecs) {
            const [schema, table, _] = columnPath.split('.')
            const colTablePath = [schema, table].join('.')
            const colLiveObjectId = liveProperty.split(':')[0]
            const linkKey = [colLiveObjectId, colTablePath].join(':')
            allLiveColumnsByLink[linkKey] = allLiveColumnsByLink[linkKey] || []
            allLiveColumnsByLink[linkKey].push({ columnPath, liveProperty })
        }

        // Get a set of the unique live columns that were going to already be upserted.
        const liveColumnIdsAlreadyPlannedForSeed = new Set()
        for (const { columnPath, liveProperty } of this.liveColumnsToUpsert) {
            liveColumnIdsAlreadyPlannedForSeed.add([columnPath, liveProperty].join(':'))
        }

        // Add any live columns associated with the links that changed to the `this.liveColumnsToUpsert`
        // array after the fact, so that users of this class will assume they need to be reseeded.
        for (const { liveObjectId, tablePath } of linksToUpsert) {
            const linkKey = [liveObjectId, tablePath].join(':')
            const liveColumnsForLink = allLiveColumnsByLink[linkKey] || []

            for (const liveColumn of liveColumnsForLink) {
                const { columnPath, liveProperty } = liveColumn
                if (liveColumnIdsAlreadyPlannedForSeed.has([columnPath, liveProperty].join(':'))) continue
                this.liveColumnsToUpsert.push(liveColumn)
            }
        }
    }

    async _getExistingLiveColumns() {
        this._getQuerySpecs()

        // Get all existing live columns for the given array of column paths.
        try {
            this.prevLiveColumns = await getLiveColumnsForColPaths(
                this.querySpecs.map((qs) => qs.columnPath)
            )
        } catch (err) {
            throw new QueryError('select', SPEC_SCHEMA_NAME, LIVE_COLUMNS_TABLE_NAME, err)
        }
    }

    _findLiveColumnsToUpsert() {
        const querySpecsToUpsert = []
        const prevLiveColumnsMap = mapBy<LiveColumn>(this.prevLiveColumns, 'columnPath')

        // Get all previous live table paths.
        const prevLiveTablePaths = new Set()
        for (const { columnPath } of this.prevLiveColumns) {
            const [schema, table, _] = columnPath.split('.')
            prevLiveTablePaths.add([schema, table].join('.'))
        }

        // Compare querySpecs and prevLiveColumns, to find:
        // -----
        // (1) The live columns that don't exist yet.
        // (2) The live columns where the data source has changed.
        // (3) The new live tables.
        const newLiveTablePaths = new Set<string>()
        for (const querySpec of this.querySpecs) {
            const isNewColPath = !prevLiveColumnsMap.hasOwnProperty(querySpec.columnPath)
            const colDataSourceChanged =
                !isNewColPath &&
                prevLiveColumnsMap[querySpec.columnPath].liveProperty !== querySpec.liveProperty

            if (isNewColPath || colDataSourceChanged) {
                querySpecsToUpsert.push(querySpec)
            }

            const [schema, table, _] = querySpec.columnPath.split('.')
            const colTablePath = [schema, table].join('.')
            if (!prevLiveTablePaths.has(colTablePath)) {
                newLiveTablePaths.add(colTablePath)
            }
        }

        this.liveColumnsToUpsert = querySpecsToUpsert
        this.newLiveTablePaths = newLiveTablePaths
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

    _getLinksInConfig(): StringKeyMap {
        const linksInConfig = {}
        for (const { columnPath, liveProperty } of this.querySpecs) {
            const [schema, table, _] = columnPath.split('.')
            const tablePath = [schema, table].join('.')
            const [liveObjectId, __] = liveProperty.split(':')

            const linkKey = [liveObjectId, tablePath].join(':')
            const link = config.getLink(liveObjectId, tablePath)
            if (!link) {
                logger.warn(`Upsert links — Link not found (tablePath=${tablePath}, liveObjectId=${liveObjectId})`)
                continue
            }

            const { uniqueBy, filterBy } = link
            linksInConfig[linkKey] = { 
                uniqueBy: uniqueBy || [], 
                filterBy: filterBy || []
            }
        }
        return linksInConfig
    }
}

export default UpsertLiveColumnsService
