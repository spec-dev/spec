import logger from '../logger'
import { Op, OpType, StringKeyMap } from '../types'
import { QueryError } from '../errors'
import { Knex } from 'knex'
import short from 'short-uuid'
import { tablesMeta } from '../db/tablesMeta'
import { pool, db } from '../db'
import { mergeByKeys, stringify } from '../utils/formatters'
import { applyDefaults } from '../defaults'
import { isJSONColType } from '../utils/colTypes'
import { constants } from '../constants'
import { sleep } from '../utils/time'
import { randomIntegerInRange } from '../utils/math' 

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
        let insertQuery = this.tx(this.tablePath)

        // Format data as an array.
        let data = Array.isArray(this.op.data) ? this.op.data : [this.op.data]
        if (!data.length) return

        // Apply any default column values configured by the user.
        if (Object.keys(this.op.defaultColumnValues).length) {
            data = applyDefaults(data, this.op.defaultColumnValues) as StringKeyMap[]
        }

        // Ensure JSON columns are JSON.
        data = this._enforceJSONColumns(data)

        // Add upsert functionality if specified.
        const conflictTargets = this.op.conflictTargets
        if (conflictTargets) {
            const uniqueData = mergeByKeys(data, conflictTargets)
            const operationColumns = Object.keys(data[0])

            // Only merge live columns that are NOT included in the conflict target.
            const mergeColNames = this.op.liveTableColumns.filter(
                (colName) =>
                    !conflictTargets.includes(colName) && operationColumns.includes(colName)
            )

            // Insert or update.
            if (mergeColNames.length) {
                insertQuery.insert(uniqueData).onConflict(conflictTargets).merge(mergeColNames)

                // Restrict updates to only forwards-in-time if the live object's 
                // primaryTimestampProperty is the source for one of the live columns.
                const timestampCol = this.op.primaryTimestampColumn
                const onlyMergeForwardInTime =
                    timestampCol &&
                    mergeColNames.includes(timestampCol)

                if (onlyMergeForwardInTime) {
                    insertQuery.whereRaw('??.??.?? <= excluded.??', [
                        this.op.schema,
                        this.op.table,
                        timestampCol,
                        timestampCol,
                    ])
                }
            }
            // Insert or ignore.
            else {
                insertQuery.insert(uniqueData).onConflict(conflictTargets).ignore()
            }
        } else {
            insertQuery.insert(data)
        }

        // Run insert/upsert with deadlock protection.
        let attempt = 1
        while (attempt <= constants.MAX_DEADLOCK_RETRIES) {
            try {
                await insertQuery
                break
            } catch (err) {
                attempt++
                const message = err.message || err.toString() || ''
            
                // Wait and try again if deadlocked.
                if (message.toLowerCase().includes('deadlock')) {
                    logger.error(
                        `Upsert on ${this.tablePath} got deadlock on attempt ${attempt}/${constants.MAX_DEADLOCK_RETRIES}.`
                    )
                    await sleep(randomIntegerInRange(50, 500))
                    continue
                }

                const queryError = new QueryError('insert', this.op.schema, this.op.table, err)
                logger.error(queryError.message)
                throw queryError
            }
        }
    }

    _enforceJSONColumns(data: StringKeyMap[]): StringKeyMap[] {
        const colTypes = tablesMeta[this.tablePath].colTypes
        const jsonColNames = []
        for (const colName in colTypes) {
            const colType = colTypes[colName]
            if (isJSONColType(colType)) {
                jsonColNames.push(colName)
            }
        }
        if (!jsonColNames.length) return data

        const newData = []
        for (const record of data) {
            const updates = {}
            for (const colName of jsonColNames) {
                updates[colName] = stringify(record[colName])
            }
            newData.push({
                ...record,
                ...updates,
            })
        }
        return newData
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
        const primaryKeyColNames = primaryKeyCols.map((pk) => pk.name)
        const primaryKeyColDefs = primaryKeyColNames.map(
            (colName) => `${colName} ${colTypes[colName]} not null`
        )
        const updateColNames = Object.keys(this.op.data[0])
        const updateColDefs = updateColNames.map((colName) => `${colName} ${colTypes[colName]}`)
        const primaryKeyConstraintDef = `constraint ${tempTableName}_pkey primary key (${primaryKeyColNames.join(
            ', '
        )})`
        const innerTableDef = [
            ...primaryKeyColDefs,
            ...updateColDefs,
            primaryKeyConstraintDef,
        ].join(', ')

        // Merge primary keys and updates into individual records.
        const tempRecords = []
        for (let i = 0; i < this.op.where.length; i++) {
            tempRecords.push({ ...this.op.where[i], ...this.op.data[i] })
        }

        // Build the bulk insert query for a temp table.
        const valueColNames = Object.keys(tempRecords[0])
        const valuePlaceholders = tempRecords
            .map((r) => `(${valueColNames.map((_) => '?').join(', ')})`)
            .join(', ')
        const valueBindings = tempRecords
            .map((r) => valueColNames.map((colName) => r[colName]))
            .flat()
        const insertQuery = db
            .raw(
                `INSERT INTO ${tempTableName} (${valueColNames.join(
                    ', '
                )}) VALUES ${valuePlaceholders}`,
                valueBindings
            )
            .toSQL()
            .toNative()

        // Which columns to merge over from the temp table and how the target table should join against it.
        const updateSet = updateColNames
            .map((colName) => `${colName} = ${tempTableName}.${colName}`)
            .join(', ')
        const updateWhere = primaryKeyColNames
            .map((colName) => `${this.tablePath}.${colName} = ${tempTableName}.${colName}`)
            .join(' AND ')

        // Since knex.js is FUCKING trash and can't understand how to
        // work with temp tables, acquire a connection from 'pg' directly.
        const client = await pool.connect()

        try {
            // Create temp table and insert updates + primary key data.
            await client.query('BEGIN')
            await client.query(
                `CREATE TEMP TABLE ${tempTableName} (${innerTableDef}) ON COMMIT DROP`
            )

            // Bulk insert the updated records to the temp table.
            await client.query(insertQuery.sql, insertQuery.bindings)

            // Merge the temp table updates into the target table ("bulk update").
            await client.query(
                `UPDATE ${this.tablePath} SET ${updateSet} FROM ${tempTableName} WHERE ${updateWhere}`
            )
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
        for (let colName in this.op.where || {}) {
            conditions.push([colName, this.op.where[colName]])
        }
        return conditions
    }
}

export default RunOpService
