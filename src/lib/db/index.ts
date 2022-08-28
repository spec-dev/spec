import knex from 'knex'
import constants from '../constants'
import createSubscriber from 'pg-listen'
import logger from '../logger'
import { Pool } from 'pg'

const connectionConfig = {
    host : constants.DB_HOST,
    port : constants.DB_PORT,
    user : constants.DB_USER,
    password : constants.DB_PASSWORD,
    database : constants.DB_NAME,
}

export const db = knex({
    client: 'pg',
    connection: connectionConfig,
})

// Create connection pool.
export const pool = new Pool(connectionConfig)
pool.on('error', err => logger.error('pg client error', err))
pool.on('drain', (...args) => logger.info('pg client drain', ...args))
pool.on('notice', (...args) => logger.info('pg client notice', ...args))
pool.on('notification', (...args) => logger.info('pg client notification', ...args))

export const pgListener = createSubscriber(connectionConfig)

pgListener.events.on('error', async err => {
    logger.error(`Table Subscriber Error: ${err}`)
    // TODO: Attempt reconnection with debouncing and exponential backoff.
})

export const schema = (name, tx?) => {
    tx = tx || db
    return tx.withSchema(name)
}