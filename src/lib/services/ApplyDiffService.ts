import { LiveObjectLink, StringKeyMap, StringMap, Op, OpType, TableDataSources, ForeignKeyConstraint } from '../types'
import config from '../config'
import { db } from '../db'
import { toMap } from '../utils/formatters'
import { getRelationshipBetweenTables } from '../db/ops'
import { QueryError } from '../errors'

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

    get linkPrimaryKeys(): string[] {
        return ['id'] // TODO Get these from some other module that gets the primary keys for input this.link.table
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

        // Find existing records that should be updated by this diff, 
        // and curate the ops for those updates.
        await this._determineQueryConditions()
        await this._findRecordsToUpdate()
        this._createUpdateOps()
        return this.ops
    }

    async runOps() {
        
    }

    async _createUpdateOps() {
        if (!this.recordsToUpdate.length) return
        this.ops = this.recordsToUpdate.map(record => this._createUpdateOp(record)).filter(val => !!val)
    }

    _createUpdateOp(record: StringKeyMap): Op | null {
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
        for (let primaryKey of this.linkPrimaryKeys) {
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

    async _determineQueryConditions() {
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
        // Start a new query on the table this live object is linked to.
        let query = db.from(this.linkTablePath)

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