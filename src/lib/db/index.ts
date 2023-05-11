import knex from 'knex'
import { constants } from '../constants'
import createSubscriber from 'pg-listen'
import logger from '../logger'
import { Pool } from 'pg'
import { StringKeyMap } from '../types'

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
