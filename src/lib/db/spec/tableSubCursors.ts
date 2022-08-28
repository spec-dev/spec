import { schema } from '..'
import { TableSubCursor } from '../../types'
import { SPEC_SCHEMA_NAME } from '.'
import logger from '../../logger'
import { unique } from '../../utils/formatters'
import { db } from '..'

export const TABLE_SUB_CURSORS_TABLE_NAME = 'table_sub_cursors'

const tableSubCursors = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(TABLE_SUB_CURSORS_TABLE_NAME)

export async function getTableSubCursorsForPaths(tablePaths: string[]): Promise<TableSubCursor[]> {
    let records
    try {
        records = await tableSubCursors()
            .select('*')
            .whereIn('table_path', unique(tablePaths))
    } catch (err) {
        logger.error(`Error getting table_sub_cursors for table_paths: ${tablePaths.join(', ')}: ${err}`)
        return []
    }
    return records || []
}

export async function upsertTableSubCursor(tablePath: string) {
    try {
        await tableSubCursors()
            .insert({
                table_path: tablePath,
                timestamp: db.raw(`CURRENT_TIMESTAMP at time zone 'UTC'`),
            })
            .onConflict('table_path')
            .merge()
    } catch (err) {
        logger.error(`Error upserting table_sub_cursors: ${err}`)
    }
}