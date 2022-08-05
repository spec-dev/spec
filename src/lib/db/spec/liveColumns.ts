import { schema } from '..'
import { LiveColumn, LiveColumnSeedStatus } from '../../types'
import { SPEC_SCHEMA_NAME } from '.'
import logger from '../../logger'
import { Knex } from 'knex'

export const LIVE_COLUMNS_TABLE_NAME = 'live_columns'

const liveColumns = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(LIVE_COLUMNS_TABLE_NAME)

export async function getLiveColumnsForColPaths(columnPaths: string[]): Promise<LiveColumn[]> {
    let records
    try {
        records = await liveColumns()
            .select('*')
            .whereIn('column_path', columnPaths)
    } catch (err) {
        logger.error(`Error getting live_columns for column_paths: ${columnPaths.join(', ')}: ${err}`)
        throw err
    }

    return (records || []).map(record => ({
        columnPath: record.column_path,
        liveProperty: record.live_property,
        seedStatus: record.seed_status as LiveColumnSeedStatus,
    })) as LiveColumn[]
}

export async function saveLiveColumns(records: LiveColumn[]) {
    try {
        await liveColumns()
            .insert(records.map(record => ({ // TODO: Just use an auto camel-to-snake case converter.
                column_path: record.columnPath,
                live_property: record.liveProperty,
                seed_status: record.seedStatus,
            })))
            .onConflict('column_path')
            .merge()
    } catch (err) {
        logger.error(`Error saving live columns: ${err}`)
        throw err
    }
}

export async function seedFailed(columnPaths: string | string[]) {
    if (!Array.isArray(columnPaths)) {
        columnPaths = [columnPaths]
    }
    try {
        await liveColumns()
            .update('seed_status', LiveColumnSeedStatus.Failed)
            .whereIn('column_path', columnPaths)
    } catch (err) {
        logger.error(`Error marking live columns as failed (ironically): ${err}`)
    }
}