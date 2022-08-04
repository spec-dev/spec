import { schema, db } from '..'
import { EventCursor } from '../../types'
import { SPEC_SCHEMA_NAME } from '.'
import logger from '../../logger'

export const EVENT_CURSORS_TABLE_NAME = 'event_cursors'

const eventCursors = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(EVENT_CURSORS_TABLE_NAME)

export async function getEventCursorsForNames(names: string[]): Promise<EventCursor[]> {
    let records
    try {
        records = await eventCursors()
            .select('*')
            .whereIn('name', names)
    } catch (err) {
        logger.error(`Error getting event_cursors for names: ${names.join(', ')}: ${err}`)
        return []
    }

    return (records || []).map(record => ({
        ...record,
        nonce: Number(record.nonce),
        timestamp: Number(record.timestamp),
    })) as EventCursor[]
}

export async function saveEventCursors(records: EventCursor[]) {
    try {
        await eventCursors()
            .insert(records)
            .onConflict('name')
            .merge()
    } catch (err) {
        logger.error(`Error saving event cursors: ${err}`)
    }
}