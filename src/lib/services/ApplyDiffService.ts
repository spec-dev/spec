import { LiveObjectLink, StringKeyMap, StringMap, Op, OpType, TableDataSources, ForeignKeyConstraint } from '../types'
import config from '../config'
import { db } from '../db'
import { toMap } from '../utils/formatters'
import { getRelationshipBetweenTables, getUniqueColGroups, getPrimaryKeys } from '../db/ops'
import { QueryError } from '../errors'
import RunOpService from './RunOpService'
import logger from '../logger'

class ApplyDiffService {

    liveObjectDiff: StringKeyMap

    link: LiveObjectLink

    liveObjectId: string

    ops: Op[] = []

    queryConditions: {
        where: any[]
        join: any[]
    } = { where: [], join: [] }

    recordsToUpdate: StringKeyMap[] = []

    tableDataSources: TableDataSources

    rels: { [key: string]: ForeignKeyConstraint | null } = {}

    get linkTablePath(): string {
        return this.link.table
    }

    get linkSchemaName(): string {
        return this.link.table.split('.')[0]
    }

    get linkTableName(): string {
        return this.link.table.split('.')[1]
    }

    get linkProperties(): StringMap {
        return toMap(this.link.properties || {})
    }

    get linkColPaths(): string[] {
        return Object.values(this.linkProperties)
    }

    constructor(diff: StringKeyMap, link: LiveObjectLink, liveObjectId: string) {
        this.liveObjectDiff = diff
        this.link = link
        this.liveObjectId = liveObjectId
        this.tableDataSources = this._getLiveObjectTableDataSources()
        this._validateLink()
    }

    async perform() {
        await this.getOps()
        await this.runOps()
    }

    async getOps(): Promise<Op[]> {
        // No ops will exist if this table isn't reliant on our live object.
        if (!Object.keys(this.tableDataSources).length) {
            return this.ops
        }

        await this._findRecordsToUpdate()

        if (this.recordsToUpdate.length > 0) {
            await this._createUpdateOps()
        } else if (this.link.eventsCanInsert) {
            await this._createInsertOps()
        }        

        return this.ops
    }

    async runOps() {
        if (!this.ops.length) return
        await db.transaction(async tx => {
            await Promise.all(this.ops.map(op => new RunOpService(op, tx).perform()))
        })
    }

    async _createInsertOps() {
        const properties = this.linkProperties
        const tablePath = this.linkTablePath

        const foreignTableQueryConditions = {}
        for (const property in properties) {
            const colPath = properties[property]
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = `${colSchemaName}.${colTableName}`
    
            if (colTablePath !== tablePath) {
                const rel = await this._getRel(tablePath, colTablePath)
                if (!rel) throw `No rel from ${tablePath} -> ${colTablePath}`

                if (!foreignTableQueryConditions.hasOwnProperty(colTablePath)) {
                    foreignTableQueryConditions[colTablePath] = {
                        rel,
                        tablePath: colTablePath,
                        where: [],
                    }
                }

                foreignTableQueryConditions[colTablePath].where.push([
                    colName,
                    this.liveObjectDiff[property],
                ])
            }
        }

        const newRecord = {}

        for (const foreignTablePath in foreignTableQueryConditions) {
            const queryConditions = foreignTableQueryConditions[foreignTablePath]
            let query = db.from(foreignTablePath)

            for (let i = 0; i < queryConditions.where.length; i++) {
                const [col, val] = queryConditions.where[i]
                i ? query.andWhere(col, val) : query.where(col, val)
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

            if (records.length > 1) {
                logger.warn(
                    `Can't insert record to ${this.linkTablePath} from event - 
                    more than one foreign record found for ${queryConditions}`
                )
                return
            }

            const foreignRecord = records[0]
            const { foreignKey, referenceKey } = queryConditions.rel
            newRecord[foreignKey] = foreignRecord[referenceKey]
        }

        for (const property in this.liveObjectDiff) {
            const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
            const value = this.liveObjectDiff[property]
            for (const { columnName } of colsWithThisPropertyAsDataSource) {
                newRecord[columnName] = value
            }
        }

        const linkTableUniqueColGroups = await getUniqueColGroups(this.linkTablePath)

        this.ops.push({
            type: OpType.Insert,
            schema: this.linkSchemaName,
            table: this.linkTableName,
            data: newRecord,
            uniqueColGroups: linkTableUniqueColGroups,
        })
    }

    async _createUpdateOps() {
        const linkTablePrimaryKeys = await getPrimaryKeys(this.linkTablePath)
        if (!linkTablePrimaryKeys.length) throw `Primary keys could not be determined for ${this.linkTablePath}`
        this.ops = this.recordsToUpdate.map(record => this._createUpdateOp(record, linkTablePrimaryKeys)).filter(val => !!val)
    }

    _createUpdateOp(record: StringKeyMap, primaryKeys: string[]): Op | null {
        // Get the properties on the diff that aren't link properties.
        const linkPropertyKeys = new Set<string>(Object.keys(this.linkProperties))
        const updateProperties = Object.keys(this.liveObjectDiff).filter(key => !linkPropertyKeys.has(key))
        if (!updateProperties.length) return null

        // For each update property (from the diff), find the column(s) that use 
        // it as a data source, and then create a map of the record updates to apply.
        const recordUpdates = {}
        for (let updateProperty of updateProperties) {
            // Find the columns that use this property key as a data source.
            const colsWithThisPropertyAsDataSource = this.tableDataSources[updateProperty] || []
            
            // Register value update for each column.
            const newValue = this.liveObjectDiff[updateProperty]
            for (const { columnName } of colsWithThisPropertyAsDataSource) {
                const currentValue = record[columnName]
                if (currentValue !== newValue) {
                    recordUpdates[columnName] = newValue
                }
            }
        }
        if (!Object.keys(recordUpdates).length) return null

        // Create the lookup/where conditions for the update.
        // These will just be the primary keys / values of this record.
        const whereConditions = {}
        for (let primaryKey of primaryKeys) {
            whereConditions[primaryKey] = record[primaryKey]
        }

        return {
            type: OpType.Update,
            schema: this.linkSchemaName,
            table: this.linkTableName,
            where: whereConditions,
            data: recordUpdates,
        }
    }

    async _getExistingRecordQueryConditions() {
        this.queryConditions = { where: [], join: [] }
        const properties = this.linkProperties
        const tablePath = this.linkTablePath

        for (const property in properties) {
            const colPath = properties[property]
            const [colSchemaName, colTableName, _] = colPath.split('.')
            const colTablePath = `${colSchemaName}.${colTableName}`
    
            // Handle foreign tables.
            if (colTablePath !== tablePath) {
                const rel = await this._getRel(tablePath, colTablePath)
                if (!rel) throw `No rel from ${tablePath} -> ${colTablePath}`

                this.queryConditions.join.push([
                    colTableName,
                    `${colTablePath}.${rel.referenceKey}`,
                    `${tablePath}.${rel.foreignKey}`,
                ])
            }
    
            this.queryConditions.where.push([
                colPath, 
                this.liveObjectDiff[property],
            ])
        }
    }

    async _findRecordsToUpdate() {
        await this._getExistingRecordQueryConditions()

        // Start a new query on the table this live object is linked to.
        let query = db.from(this.linkTablePath).select([`${this.linkTablePath}.*`])

        // Add JOIN conditions.
        for (let join of this.queryConditions.join) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, joinRefKey, joinForeignKey)
        }

        // Add WHERE conditions.
        for (let i = 0; i < this.queryConditions.where.length; i++) {
            const [col, val] = this.queryConditions.where[i]
            i ? query.andWhere(col, val) : query.where(col, val)
        }

        // Perform the query, finding existing records that should be updated.
        try {
            this.recordsToUpdate = await query
        } catch (err) {
            throw new QueryError('select', this.linkSchemaName, this.linkTableName, err)
        }
    }

    _validateLink() {
        // Ensure linked table has a valid path structure.
        if (!this.linkSchemaName || !this.linkTableName) {
            throw `Invalid live object link -- table path invalid: ${this.linkTablePath}`
        }
    
        // Ensure linked properties exist.
        const properties = this.linkProperties
        if (!Object.keys(this.linkProperties).length) {
            throw `Invalid live object link -- properties map is empty: ${this.linkTablePath}`
        }
    
        // Ensure all linked column paths are valid.
        for (const property in properties) {
            const colPath = properties[property]
            if (!colPath || colPath.split('.').length !== 3) {
                throw `Invalid live object link -- invalid column path for property ${property}: ${colPath}`
            }
        }
    }

    _getLiveObjectTableDataSources(): TableDataSources {
        const allTableDataSources = config.getDataSourcesForTable(this.linkSchemaName, this.linkTableName) || {}
        const tableDataSourcesForThisLiveObject = {}

        // Basically just recreate the map, but filtering out the data sources that 
        // aren't associated with our live object. Additionally, use just the live 
        // object property as the new key (removing the live object id).
        for (let key in allTableDataSources) {
            const [liveObjectId, property] = key.split(':')
            if (liveObjectId !== this.liveObjectId) continue
            tableDataSourcesForThisLiveObject[property] = allTableDataSources[key]
        }

        return tableDataSourcesForThisLiveObject
    }

    async _getRel(tablePath: string, foreignTablePath: string): Promise<ForeignKeyConstraint | null> {
        const key = [tablePath, foreignTablePath].join(':')

        if (this.rels.hasOwnProperty(key)) {
            return this.rels[key]
        }
        
        const rel = await getRelationshipBetweenTables(tablePath, foreignTablePath)
        this.rels[key] = rel
        return rel
    }
}

export default ApplyDiffService