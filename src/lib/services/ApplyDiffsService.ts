import { LiveObjectLink, LiveObject, StringKeyMap, StringMap, Op, OpType, TableDataSources, ForeignKeyConstraint } from '../types'
import config from '../config'
import { db } from '../db'
import { toMap, unique } from '../utils/formatters'
import { QueryError } from '../errors'
import RunOpService from './RunOpService'
import { getRel, tablesMeta } from '../db/tablesMeta'
import logger from '../logger'
import constants from '../constants'

const valueSep = '__:__'

class ApplyDiffsService {

    liveObjectDiffs: StringKeyMap[]

    link: LiveObjectLink

    liveObject: LiveObject

    ops: Op[] = []

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
        return toMap(this.link.linkOn || {})
    }

    get canInsertRecords(): boolean {
        const link = toMap(this.link)
        return link.hasOwnProperty('eventsCanInsert') ? !!link.eventsCanInsert : true
    }

    get tableDataSources(): TableDataSources {
        return config.getLiveObjectTableDataSources(this.liveObject.id, this.linkTablePath)
    }

    get tablePrimaryKeys(): string[] {
        const meta = tablesMeta[this.linkTablePath]
        if (!meta) throw `No meta registered for table ${this.linkTablePath}`
        return meta.primaryKey.map(pk => pk.name)
    }

    get linkTableUniqueConstraint(): string[] {
        const uniqueConstaint = config.getUniqueConstraintForLink(this.liveObject.id, this.linkTablePath)
        if (!uniqueConstaint) throw `No unique constraint for link ${this.liveObject.id} <-> ${this.linkTablePath}`
        return uniqueConstaint
    }

    get defaultFilters(): StringKeyMap {
        return this.liveObject.filterBy || {}
    }
    
    constructor(diffs: StringKeyMap[], link: LiveObjectLink, liveObject: LiveObject) {
        this.liveObjectDiffs = diffs
        this.link = link
        this.liveObject = liveObject
    }

    async perform() {
        await this.getOps()
        await this.runOps()
    }

    async getOps(): Promise<Op[]> {
        // Ensure table is reliant on this live object.
        if (!Object.keys(this.tableDataSources).length) {
            logger.error(`Table ${this.linkTablePath} isn't reliant on Live Object ${this.liveObject.id}.`)
            return this.ops
        }

        // Upsert or Update records using diffs.
        await (this.canInsertRecords ? this._createUpsertOps() : this._createUpdateOps())
        
        return this.ops
    }

    async runOps() {
        if (!this.ops.length) return
        await db.transaction(async tx => {
            await Promise.all(this.ops.map(op => new RunOpService(op, tx).perform()))
        })
    }

    async _createUpsertOps() {
        const properties = this.linkProperties
        const tablePath = this.linkTablePath
        const tableDataSources = this.tableDataSources

        // Get query conditions for the linked foreign tables.
        const foreignTableQueryConditions = {}
        for (const property in properties) {
            const colPath = properties[property]
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = `${colSchemaName}.${colTableName}`
            if (colTablePath === tablePath) continue

            const rel = getRel(tablePath, colTablePath)
            if (!rel) throw `No rel from ${tablePath} -> ${colTablePath}`

            if (!foreignTableQueryConditions.hasOwnProperty(colTablePath)) {
                foreignTableQueryConditions[colTablePath] = {
                    rel,
                    tablePath: colTablePath,
                    whereIn: [],
                    properties: [],
                    colNames: [],
                }
            }

            foreignTableQueryConditions[colTablePath].properties.push(property)
            foreignTableQueryConditions[colTablePath].colNames.push(colName)
            foreignTableQueryConditions[colTablePath].whereIn.push([
                colName,
                unique(this.liveObjectDiffs.map(diff => diff[property])),
            ])
        }

        // Find foreign table records potentially needed for reference during inserts.
        const referenceKeyValues = {}
        for (const foreignTablePath in foreignTableQueryConditions) {
            const queryConditions = foreignTableQueryConditions[foreignTablePath]
            let query = db.from(foreignTablePath)

            for (let i = 0; i < queryConditions.whereIn.length; i++) {
                const [col, vals] = queryConditions.whereIn[i]
                query.whereIn(col, vals)
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

            referenceKeyValues[foreignTablePath] = {}
            for (const record of records) {
                const key = queryConditions.colNames.map(colName => record[colName]).join(valueSep)
                if (referenceKeyValues[foreignTablePath].hasOwnProperty(key)) continue
                referenceKeyValues[foreignTablePath][key] = record[queryConditions.rel.referenceKey]
            }
        }

        // Format record objects to upsert.
        const upsertRecords = []
        for (const diff of this.liveObjectDiffs) {
            const upsertRecord = {}
            for (const property in diff) {
                const colsWithThisPropertyAsDataSource = tableDataSources[property] || []
                const value = diff[property]
                for (const { columnName } of colsWithThisPropertyAsDataSource) {
                    upsertRecord[columnName] = value
                }
            }

            let ignoreDiff = false
            for (const foreignTablePath in foreignTableQueryConditions) {
                const queryConditions = foreignTableQueryConditions[foreignTablePath]
                const uniqueForeignRefKey = queryConditions.properties.map(property => diff[property]).join(valueSep)
                if (!referenceKeyValues[foreignTablePath].hasOwnProperty(uniqueForeignRefKey)) {
                    ignoreDiff = true
                    break
                }
                const referenceKeyValue = referenceKeyValues[foreignTablePath][uniqueForeignRefKey]
                upsertRecord[queryConditions.rel.foreignKey] = referenceKeyValue
            }
            if (ignoreDiff) continue

            upsertRecords.push(upsertRecord)
        }

        // Perform all upserts in a single, bulk insert operation.
        this.ops = [{
            type: OpType.Insert,
            schema: this.linkSchemaName,
            table: this.linkTableName,
            data: upsertRecords,
            conflictTargets: this.linkTableUniqueConstraint,
        }]
    }

    async _createUpdateOps() {
        await (this.liveObjectDiffs > constants.MAX_UPDATES_BEFORE_BULK_UPDATE_USED
            ? this._createBulkUpdateOp() 
            : this._createIndividualUpdateOps())
    }

    async _createIndividualUpdateOps() {
        const properties = this.linkProperties
        const tablePath = this.linkTablePath
        const tableDataSources = this.tableDataSources

        // Get query conditions for the linked foreign tables.
        const foreignTableQueryConditions = {}
        for (const property in properties) {
            const colPath = properties[property]
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = `${colSchemaName}.${colTableName}`
            if (colTablePath === tablePath) continue

            const rel = getRel(tablePath, colTablePath)
            if (!rel) throw `No rel from ${tablePath} -> ${colTablePath}`

            if (!foreignTableQueryConditions.hasOwnProperty(colTablePath)) {
                foreignTableQueryConditions[colTablePath] = {
                    rel,
                    tablePath: colTablePath,
                    whereIn: [],
                    properties: [],
                    colNames: [],
                }
            }

            foreignTableQueryConditions[colTablePath].properties.push(property)
            foreignTableQueryConditions[colTablePath].colNames.push(colName)
            foreignTableQueryConditions[colTablePath].whereIn.push([
                colName,
                unique(this.liveObjectDiffs.map(diff => diff[property])),
            ])
        }

        // Find foreign table records potentially needed for reference.
        const referenceKeyValues = {}
        for (const foreignTablePath in foreignTableQueryConditions) {
            const queryConditions = foreignTableQueryConditions[foreignTablePath]
            let query = db.from(foreignTablePath)

            for (let i = 0; i < queryConditions.whereIn.length; i++) {
                const [col, vals] = queryConditions.whereIn[i]
                query.whereIn(col, vals)
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

            referenceKeyValues[foreignTablePath] = {}
            for (const record of records) {
                const key = queryConditions.colNames.map(colName => record[colName]).join(valueSep)
                if (referenceKeyValues[foreignTablePath].hasOwnProperty(key)) continue
                referenceKeyValues[foreignTablePath][key] = record[queryConditions.rel.referenceKey]
            }
        }

        // Format record objects to update.
        for (const diff of this.liveObjectDiffs) {
            const updates = {}
            const where = {}
            for (const property in diff) {
                const value = diff[property]
                // If this is a linked property...
                if (properties.hasOwnProperty(property)) {
                    const linkedColPath = properties[property]
                    const [colSchemaName, colTableName, colName] = linkedColPath.split('.')
                    const colTablePath = `${colSchemaName}.${colTableName}`
                    if (colTablePath === tablePath) {
                        where[colName] = value
                    }
                } else {
                    const colsWithThisPropertyAsDataSource = tableDataSources[property] || []
                    for (const { columnName } of colsWithThisPropertyAsDataSource) {
                        updates[columnName] = value
                    }
                }
            }
            if (!Object.keys(updates).length) continue

            let ignoreDiff = false
            for (const foreignTablePath in foreignTableQueryConditions) {
                const queryConditions = foreignTableQueryConditions[foreignTablePath]
                const uniqueForeignRefKey = queryConditions.properties.map(property => diff[property]).join(valueSep)
                if (!referenceKeyValues[foreignTablePath].hasOwnProperty(uniqueForeignRefKey)) {
                    ignoreDiff = true
                    break
                }
                const referenceKeyValue = referenceKeyValues[foreignTablePath][uniqueForeignRefKey]
                where[queryConditions.rel.foreignKey] = referenceKeyValue
            }
            if (ignoreDiff) continue
            
            this.ops.push({
                type: OpType.Update,
                schema: this.linkSchemaName,
                table: this.linkTableName,
                where,
                data: updates,
            })
        }
    }

    async _createBulkUpdateOp() {
        const queryConditions = this._getExistingRecordQueryConditions()
        
        // Start a new query on the linked table.
        let query = db.from(this.linkTablePath)

        // Add JOIN conditions.
        for (let join of queryConditions.join) {
            const [joinTable, joinRefKey, joinForeignKey] = join
            query.innerJoin(joinTable, joinRefKey, joinForeignKey)
        }

        // Add SELECT conditions.
        query.select(queryConditions.select)

        // Add WHERE-IN conditions.
        for (let i = 0; i < queryConditions.whereIn.length; i++) {
            const [col, vals] = queryConditions.whereIn[i]
            query.whereIn(col, vals)
        }

        let records = []
        try {
            records = await query
        } catch (err) {
            throw new QueryError('select', this.linkSchemaName, this.linkTableName, err)
        }
        if (!records.length) return

        const uniqueDiffs = {}
        for (const diff of this.liveObjectDiffs) {
            const uniqueKeyComps = []
            for (const linkPropertyKey of this.link.uniqueBy) {
                const value = diff[linkPropertyKey] || ''
                uniqueKeyComps.push(value)
            }
            const uniqueKey = uniqueKeyComps.join(valueSep)
            uniqueDiffs[uniqueKey] = { ...(uniqueDiffs[uniqueKey] || {}), ...diff }
        }

        const linkProperties = this.linkProperties
        const where = []
        const updates = []
        const tablePrimaryKeys = this.tablePrimaryKeys
        const tableDataSources = this.tableDataSources

        for (const record of records) {
            // Get the diff associated with this record (if exists).
            const uniqueKeyComps = []
            let ignoreRecord = false
            for (const linkPropertyKey of this.link.uniqueBy) {
                const colPath = linkProperties[linkPropertyKey]
                if (!colPath) {
                    ignoreRecord = true
                    break
                }
                const [colSchemaName, colTableName, colName] = colPath.split('.')
                const colTablePath = `${colSchemaName}.${colTableName}`    
                const value = record[colTablePath === this.linkTablePath ? colName : colPath] || ''
                uniqueKeyComps.push(value)
            }
            if (ignoreRecord) continue
            const uniqueKey = uniqueKeyComps.join(valueSep)
            const diff = uniqueDiffs[uniqueKey]
            if (!diff) continue

            // Build a record updates map using the diff, ignoring any linked properties.
            const recordUpdates = {}
            for (const property in tableDataSources) {
                if (linkProperties.hasOwnProperty(property) || !diff.hasOwnProperty(property)) continue
                const colNames = tableDataSources[property].map(ds => ds.columnName)
                for (const colName of colNames) {
                    recordUpdates[colName] = diff[property]
                }
            }
            if (!Object.keys(recordUpdates).length) continue

            // Create the lookup/where conditions for the update.
            // These will just be the primary keys / values of this record.
            const pkConditions = {}
            for (let primaryKey of tablePrimaryKeys) {
                pkConditions[primaryKey] = record[primaryKey]
            }

            updates.push(recordUpdates)
            where.push(pkConditions)
        }

        this.ops.push({
            type: OpType.Update,
            schema: this.linkSchemaName,
            table: this.linkTableName,
            where,
            data: updates,
        })
    }

    _getExistingRecordQueryConditions(): StringKeyMap {
        const queryConditions = { 
            join: [],
            select: [`${this.linkTablePath}.*`],
            whereIn: [],
        }
        const properties = this.linkProperties
        const tablePath = this.linkTablePath

        for (const property in properties) {
            const colPath = properties[property]
            const [colSchemaName, colTableName, colName] = colPath.split('.')
            const colTablePath = `${colSchemaName}.${colTableName}`

            if (colTablePath === tablePath) {
                queryConditions.whereIn.push([
                    colName,
                    unique(this.liveObjectDiffs.map(diff => diff[property])),
                ])
            } else {
                const rel = getRel(tablePath, colTablePath)
                if (!rel) throw `No rel from ${tablePath} -> ${colTablePath}`

                queryConditions.join.push([
                    colTableName,
                    `${colTablePath}.${rel.referenceKey}`,
                    `${tablePath}.${rel.foreignKey}`,
                ])
                
                queryConditions.select.push(`${colPath} as ${colPath}`)
                queryConditions.whereIn.push([
                    colPath,
                    unique(this.liveObjectDiffs.map(diff => diff[property])),
                ])
            }
        }
        return queryConditions
    }
}

export default ApplyDiffsService