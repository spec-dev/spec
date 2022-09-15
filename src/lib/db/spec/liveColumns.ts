import { schema, db } from '..'
import { LiveColumn } from '../../types'
import { SPEC_SCHEMA_NAME, LIVE_COLUMNS_TABLE_NAME } from './names'
import logger from '../../logger'
import { unique } from '../../utils/formatters'
import { camelizeKeys, decamelizeKeys } from 'humps'

const liveColumns = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(LIVE_COLUMNS_TABLE_NAME)

export async function getLiveColumnsForColPaths(columnPaths: string[]): Promise<LiveColumn[]> {
    let records
    try {
        records = await liveColumns()
            .select('*')
            .whereIn('column_path', unique(columnPaths))
    } catch (err) {
        logger.error(`Error getting live_columns for column_paths: ${columnPaths.join(', ')}: ${err}`)
        throw err
    }

    return camelizeKeys(records || []) as LiveColumn[]
}

export async function saveLiveColumns(records: LiveColumn[]) {
    try {
        await db.transaction(async tx => {
            await liveColumns(tx)
                .insert(decamelizeKeys(records))
                .onConflict('column_path')
                .merge()
        })
    } catch (err) {
        logger.error(`Error saving live columns: ${err}`)
        throw err
    }
}