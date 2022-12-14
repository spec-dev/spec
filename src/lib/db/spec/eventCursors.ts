import { schema, db } from '..'
import { EventCursor } from '../../types'
import { SPEC_SCHEMA_NAME, EVENT_CURSORS_TABLE_NAME } from './names'
import logger from '../../logger'
import { unique } from '../../utils/formatters'

export const eventCursors = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(EVENT_CURSORS_TABLE_NAME)

export async function getEventCursorsForNames(names: string[]): Promise<EventCursor[]> {
    if (!names.length) return []
    let records
    try {
        records = await eventCursors().select('*').whereIn('name', unique(names))
    } catch (err) {
        logger.error(`Error getting event_cursors for names: ${names.join(', ')}: ${err}`)
        return []
    }

    return (records || []).map((record) => ({
        ...record,
        timestamp: (record.timestamp as Date).toISOString(),
    })) as EventCursor[]
}

export async function saveEventCursors(records: EventCursor[]) {
    if (!records.length) return
    try {
        await db.transaction(async (tx) => {
            await eventCursors(tx).insert(records).onConflict('name').merge()
        })
    } catch (err) {
        logger.error(`Error saving event cursors: ${err}`)
    }
}
