import knex from 'knex'
import { constants } from '../constants'
import createSubscriber from 'pg-listen'
import logger from '../logger'
import { Pool } from 'pg'
import { StringKeyMap } from '../types'
import { unique } from '../utils/formatters'
import { QueryError } from '../errors'

export const connectionConfig: StringKeyMap = {
    host: constants.DB_HOST,
    port: constants.DB_PORT,
    user: constants.DB_USER,
    password: constants.DB_PASSWORD,
    database: constants.DB_NAME,
}
if (constants.DB_SSL) {
    connectionConfig.ssl = true
}

export const db = knex({
    client: 'pg',
    connection: connectionConfig,
    pool: {
        min: 0,
        max: constants.MAX_POOL_SIZE,
        propagateCreateError: false,
    },
})

// Create connection pool.
export const pool = new Pool({
    ...connectionConfig,
    min: 0,
    max: 10,
})
pool.on('error', (err) => logger.error('pg client error', err))

export const pgListener = createSubscriber(connectionConfig)

pgListener.events.on('error', async (err) => {
    logger.error(`Table Subscriber Error: ${err}`)
})

export const schema = (name, tx?) => {
    tx = tx || db
    return tx.withSchema(name)
}


export async function doesSchemaExist(schema: string): Promise<boolean> {
    const result = await db
        .from('information_schema.schemata')
        .where({ schema_name: schema })
        .count()
    const count = result ? Number((result[0] || {}).count || 0) : 0
    return count > 0
}

export async function doesTableExist(table: string, schema: string): Promise<boolean> {
    const result = await db
        .from('pg_tables')
        .where({ schemaname: schema, tablename: table })
        .count()
    const count = result ? Number((result[0] || {}).count || 0) : 0
    return count > 0
}

export async function areColumnsEmpty(tablePath: string, colNames: string[]): Promise<boolean> {
    const whereConditions = {}
    for (let colName of colNames) {
        whereConditions[colName] = null
    }
    const result = await db.from(tablePath).whereNot(whereConditions).count()
    const count = result ? Number((result[0] || {}).count || 0) : 0
    return count === 0
}

export async function isTableEmpty(tablePath: string): Promise<boolean> {
    return (await tableCount(tablePath)) === 0
}

export async function tableCount(tablePath: string): Promise<number> {
    const result = await db.from(tablePath).count()
    return result ? Number((result[0] || {}).count || 0) : 0
}

export async function getRecordsForPrimaryKeys(
    tablePath: string,
    primaryKeyData: StringKeyMap[]
): Promise<StringKeyMap[]> {
    // Group primary keys into arrays of values for the same key.
    const primaryKeys = {}
    Object.keys(primaryKeyData[0]).forEach((key) => {
        primaryKeys[key] = []
    })
    for (const pkData of primaryKeyData) {
        for (const key in pkData) {
            const val = pkData[key]
            primaryKeys[key].push(val)
        }
    }

    // Build query for all records associated with the array of primary keys.
    let query = db.from(tablePath).select('*')
    for (const key in primaryKeys) {
        query.whereIn(key, unique(primaryKeys[key]))
    }
    query.limit(primaryKeyData.length)

    try {
        return await query
    } catch (err) {
        const [schema, table] = tablePath.split('.')
        throw new QueryError('select', schema, table, err)
    }
}