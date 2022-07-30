import knex from 'knex'
import constants from '../constants'

export const db = knex({
    client: 'pg',
    connection: {
        host : constants.DB_HOST,
        port : constants.DB_PORT,
        user : constants.DB_USER,
        password : constants.DB_PASSWORD,
        database : constants.DB_NAME,
    },
})

export const schema = (name, tx?) => {
    tx = tx || db
    return tx.withSchema(name)
}