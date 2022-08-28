import Spec from './spec'
import { db } from './lib/db'
import { eventCursors } from './lib/db/spec'

async function start() {
    // new Spec().start()

    const eventCursor = (await eventCursors().select('*').where({ id: 'id' }))[0]
    // console.log(eventCursor)

    const recordsUpdatedAfterLastCursor = await db
        .from('spec.event_cursors')
        .select('*')
        .where(db.raw(`timezone('UTC', timestamp) >= ?`, [eventCursor.timestamp]))

    console.log(recordsUpdatedAfterLastCursor)
}

start()