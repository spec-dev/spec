import logger from '../logger'
import { Op, OpType } from '../types'
import { QueryError } from '../errors'
import { Knex } from 'knex'
import short from 'short-uuid'
import { tablesMeta } from '../db/tablesMeta'
import { pool, db } from '../db'

class RunOpService {

    op: Op

    tx: Knex.Transaction

    get tablePath(): string {
        return `${this.op.schema}.${this.op.table}`
    }

    constructor(op: Op, tx?: Knex.Transaction) {
        this.op = op
        this.tx = tx
    }

    async perform() {
        switch (this.op.type) {
            case OpType.Insert:
                await this._runInsert()
                break
            case OpType.Update:
                await this._runUpdate()
                break
            default:
                logger.error(`Unknown op type: ${this.op.type}`)
        }
    }

    async _runInsert() {
        // Start a new insert query for this table.
        let insertQuery = this.tx(this.tablePath).insert(this.op.data)

        // Add upsert functionality if specified.
        if (this.op.conflictTargets) {
            insertQuery.onConflict(this.op.conflictTargets).merge()
        }
            
        // Perform the query, inserting the record(s).
        try {
            await insertQuery
        } catch (err) {
            throw new QueryError('insert', this.op.schema, this.op.table, err)
        }
    }

    async _runUpdate() {
        await (Array.isArray(this.op.where) ? this._runBulkUpdate() : this._runIndividualUpdate())
    }

    async _runIndividualUpdate() {
        const whereConditions = this._getWhereConditionsAsList()

        // Start a new update query for this table.
        let updateQuery = this.tx(this.tablePath).update(this.op.data)

        // Add WHERE conditions.
        for (let i = 0; i < whereConditions.length; i++) {
            const [col, val] = whereConditions[i]
            i ? updateQuery.andWhere(col, val) : updateQuery.where(col, val)
        }

        // Perform the query, updating the record.
        try {
            await updateQuery
        } catch (err) {
            throw new QueryError('update', this.op.schema, this.op.table, err)
        }
    }

    async _runBulkUpdate() {
        const tempTableName = `${this.op.table}_${short.generate()}`
        const colTypes = tablesMeta[this.tablePath].colTypes
        const primaryKeyCols = tablesMeta[this.tablePath].primaryKey
        const primaryKeyColNames = primaryKeyCols.map(pk => pk.name)
        const primaryKeyColDefs = primaryKeyColNames.map(colName => `${colName} ${colTypes[colName]} not null`)
        const updateColNames = Object.keys(this.op.data[0])
        const updateColDefs = updateColNames.map(colName => `${colName} ${colTypes[colName]}`)
        const primaryKeyConstraintDef = `constraint ${tempTableName}_pkey primary key (${primaryKeyColNames.join(', ')})`
        const innerTableDef = [...primaryKeyColDefs, ...updateColDefs, primaryKeyConstraintDef].join(', ')

        // Merge primary keys and updates into individual records.
        const tempRecords = []
        for (let i = 0; i < this.op.where.length; i++) {
            tempRecords.push({ ...this.op.where[i], ...this.op.data[i] })
        }

        // Build the bulk insert query for a temp table.
        const valueColNames = Object.keys(tempRecords[0])
        const valuePlaceholders = tempRecords.map(r => `(${valueColNames.map(_ => '?').join(', ')})`).join(', ')
        const valueBindings = tempRecords.map(r => valueColNames.map(colName => r[colName])).flat()
        const insertQuery = db.raw(
            `INSERT INTO ${tempTableName} (${valueColNames.join(', ')}) VALUES ${valuePlaceholders}`,
            valueBindings,
        ).toSQL().toNative()

        // Which columns to merge over from the temp table and how the target table should join against it.
        const updateSet = updateColNames.map(colName => `${colName} = ${tempTableName}.${colName}`).join(', ')
        const updateWhere = primaryKeyColNames.map(
            colName => `${this.tablePath}.${colName} = ${tempTableName}.${colName}`
        ).join(' AND ')

        // Since knex.js is FUCKING trash and can't understand how to
        // work with temp tables, acquire a connection from 'pg' directly.
        const client = await pool.connect()

        try {
            // Create temp table and insert updates + primary key data.
            await client.query('BEGIN')
            await client.query(`CREATE TEMP TABLE ${tempTableName} (${innerTableDef}) ON COMMIT DROP`)
            
            // Bulk insert the updated records to the temp table.
            await client.query(insertQuery.sql, insertQuery.bindings)

            // Merge the temp table updates into the target table ("bulk update").
            await client.query(`UPDATE ${this.tablePath} SET ${updateSet} FROM ${tempTableName} WHERE ${updateWhere}`)
            await client.query('COMMIT')
        } catch (e) {
            await client.query('ROLLBACK')
            throw e
        } finally {
            client.release()
        }
    }

    _getWhereConditionsAsList(): string[][] {
        let conditions = []
        for (let colName in (this.op.where || {})) {
            conditions.push([colName, this.op.where[colName]])
        }
        return conditions
    }
}

export default RunOpService