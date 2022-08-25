import { LiveObjectLink, LiveObject, StringKeyMap, StringMap, Op, OpType, TableDataSources, ForeignKeyConstraint } from '../types'
import config from '../config'
import { db } from '../db'
import { toMap } from '../utils/formatters'
import { QueryError } from '../errors'
import RunOpService from './RunOpService'
import { getRel } from '../db/tablesMeta'
import logger from '../logger'

const valueSep = '__:__'

class ApplyDiffsService {

    liveObjectDiffs: StringKeyMap[]

    link: LiveObjectLink

    liveObject: LiveObject

    linkTableUniqueConstraint: string[] = []

    ops: Op[] = []

    tableDataSources: TableDataSources

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

    get canInsertRecords(): boolean {
        return !!this.link.eventsCanInsert
    }

    constructor(diffs: StringKeyMap[], link: LiveObjectLink, liveObject: LiveObject) {
        this.liveObjectDiffs = diffs
        this.link = link
        this.liveObject = liveObject
        this.tableDataSources = config.getLiveObjectTableDataSources(this.liveObject.id, this.linkTablePath)
        this.linkTableUniqueConstraint = config.getUniqueConstraintForLink(this.liveObject.id, this.linkTablePath)
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
                this.liveObjectDiffs.map(diff => diff[property]),
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
                const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
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
        const properties = this.linkProperties
        const tablePath = this.linkTablePath

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
                this.liveObjectDiffs.map(diff => diff[property]),
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
                    const colsWithThisPropertyAsDataSource = this.tableDataSources[property] || []
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

            console.log('Updating records', updates, where)
            
            this.ops.push({
                type: OpType.Update,
                schema: this.linkSchemaName,
                table: this.linkTableName,
                where: where,
                data: updates,
            })
        }
    }
}

export default ApplyDiffsService