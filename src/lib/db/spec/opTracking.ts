import { schema, db } from '..'
import { TableSubCursor } from '../../types'
import { OP_TRACKING_TABLE_NAME, SPEC_SCHEMA_NAME } from './names'
import logger from '../../logger'
import { unique } from '../../utils/formatters'
import { camelizeKeys, decamelizeKeys } from 'humps'
import { StringKeyMap } from '@spec.dev/event-client'

const opTrackingTable = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(OP_TRACKING_TABLE_NAME)

const CONFLICT_COLUMNS = [
    'table_path',
    'chain_id',
]

export async function getOpTrackingEntriesForTablePaths(tablePaths: string[]): Promise<TableSubCursor[]> {
    if (!tablePaths.length) return []
    let records
    try {
        records = await opTrackingTable().select('*').whereIn('table_path', unique(tablePaths))
    } catch (err) {
        logger.error(
            `Error getting op_tracking records for table_paths: ${tablePaths.join(', ')}: ${err}`
        )
        return []
    }
    return camelizeKeys(records || [])
}

export async function upsertOpTrackingEntries(
    entries: StringKeyMap[],
    overwrite: boolean = true,
) {
    try {
        const query = opTrackingTable().insert(decamelizeKeys(entries))
        if (overwrite) {
            await query.onConflict(CONFLICT_COLUMNS).merge()
        } else {
            await query.onConflict(CONFLICT_COLUMNS).ignore()
        }
    } catch (err) {
        logger.error(`Error upserting op_tracking entries: ${err}`)
    }
}