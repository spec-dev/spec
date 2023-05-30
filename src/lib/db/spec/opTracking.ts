import { schema } from '..'
import { OP_TRACKING_TABLE_NAME, SPEC_SCHEMA_NAME } from './names'
import logger from '../../logger'
import { decamelizeKeys } from 'humps'
import { StringKeyMap } from '../../types'

const opTrackingTable = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(OP_TRACKING_TABLE_NAME)

const CONFLICT_COLUMNS = ['table_path', 'chain_id']

export async function upsertOpTrackingEntries(
    entries: StringKeyMap[],
    overwrite: boolean = true
): Promise<boolean> {
    try {
        const query = opTrackingTable().insert(decamelizeKeys(entries))
        if (overwrite) {
            await query.onConflict(CONFLICT_COLUMNS).merge()
        } else {
            await query.onConflict(CONFLICT_COLUMNS).ignore()
        }
        return true
    } catch (err) {
        logger.error(`Error upserting op_tracking entries: ${err}`)
        return false
    }
}
