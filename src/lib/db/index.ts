import knex from 'knex'
import constants from '../constants'
import createSubscriber from 'pg-listen'
import logger from '../logger'

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

export const tableSubscriber = createSubscriber(connectionConfig)

tableSubscriber.events.on('error', async err => {
    logger.error(`Table Subscriber Error: ${err}`)

    // TODO: Attempt reconnection with debouncing and exponential backoff.
})

export const schema = (name, tx?) => {
    tx = tx || db
    return tx.withSchema(name)
}