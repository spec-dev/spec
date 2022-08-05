import logger from '../../logger'
import { doesSchemaExist, doesTableExist } from '../ops'
import { EVENT_CURSORS_TABLE_NAME } from './eventCursors'
import { LIVE_COLUMNS_TABLE_NAME } from './liveColumns'
export * from './eventCursors'
export * from './liveColumns'

export const SPEC_SCHEMA_NAME = 'spec'

const specSchemaTableNames = [
    EVENT_CURSORS_TABLE_NAME,
    LIVE_COLUMNS_TABLE_NAME,
]

const NOT_READY_DELAY = 30000 // ms

export async function ensureSpecSchemaIsReady() {
    while (true) {
        if (await specSchemaAndTablesExist()) {
            break
        }
    }
}

function specSchemaAndTablesExist(): Promise<boolean> {
    return new Promise(async (res, _) => {
        // Ensure 'spec' schema exists.
        if (!(await doesSchemaExist(SPEC_SCHEMA_NAME))) {
            logger.warn(`Schema "spec" does not yet exist. Checking again in ${NOT_READY_DELAY / 1000}s...`)
            setTimeout(() => res(false), NOT_READY_DELAY)
            return
        }

        // Ensure all required tables within the 'spec' schema exist.
        let promises = []
        for (let tableName of specSchemaTableNames) {
            promises.push(doesTableExist(tableName, SPEC_SCHEMA_NAME))
        }

        // Get an array of table names that should exist but don't.
        const tableExistence = await Promise.all(promises)
        const tablesThatDontExist = []
        for (let i = 0; i < specSchemaTableNames.length; i++) {
            if (!tableExistence[i]) {
                tablesThatDontExist.push(`"${SPEC_SCHEMA_NAME}.${specSchemaTableNames[i]}"`)
            }
        }
        if (tablesThatDontExist.length > 0) {
            logger.warn(`Tables ${tablesThatDontExist.join(', ')} do not yet exist. Checking again in ${NOT_READY_DELAY / 1000}s...`)
            setTimeout(() => res(false), NOT_READY_DELAY)
            return
        }
        
        res(true)
    })
}