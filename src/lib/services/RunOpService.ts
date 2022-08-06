import logger from '../logger'
import { Op, OpType } from '../types'
import { QueryError } from '../errors'
import { Knex } from 'knex'

class RunOpService {

    op: Op

    tx: Knex.Transaction

    get tablePath(): string {
        return `${this.op.schema}.${this.op.table}`
    }

    constructor(op: Op, tx: Knex.Transaction) {
        this.op = op
        this.tx = tx
    }

    async perform() {
        switch (this.op.type) {
            case OpType.Update:
                await this._runUpdate()
                break
            default:
                logger.error(`Unknown op type: ${this.op.type}`)
        }
    }

    async _runUpdate() {
        const whereConditions = this._getWhereConditionsAsList()
        const prettyWhereConditions = whereConditions.map(c => c.join('=')).join(', ')
        
        logger.info(`Updating ${this.tablePath} where ${prettyWhereConditions}...`)

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

    _getWhereConditionsAsList(): string[][] {
        let conditions = []
        for (let colName in this.op.where) {
            conditions.push([colName, this.op.where[colName]])
        }
        return conditions
    }
}

export default RunOpService