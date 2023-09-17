import logger from '../../logger'
import { doesSchemaExist, doesTableExist } from '..'
import { SPEC_SCHEMA_NAME, specSchemaTableNames } from './names'
export * from './eventCursors'
export * from './liveColumns'
export * from './links'
export * from './tableSubCursors'
export * from './seedCursors'
export * from './names'
export * from './ops'
export * from './opTracking'
export * from './frozenTables'

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
            logger.warn(
                `Schema "spec" does not yet exist. Checking again in ${NOT_READY_DELAY / 1000}s...`
            )
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
            logger.warn(
                `Tables ${tablesThatDontExist.join(', ')} do not yet exist. Checking again in ${
                    NOT_READY_DELAY / 1000
                }s...`
            )
            setTimeout(() => res(false), NOT_READY_DELAY)
            return
        }

        res(true)
    })
}
