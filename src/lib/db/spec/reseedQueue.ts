import { schema, db } from '..'
import { RESEED_QUEUE_TABLE_NAME, SPEC_SCHEMA_NAME } from './names'
import logger from '../../logger'
import { unique } from '../../utils/formatters'

export const reseedQueue = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(RESEED_QUEUE_TABLE_NAME)

export type ReseedJob = {
    id: number
    tableName: string
    columnNames: string
    createdAt: Date
    status: ReseedStatus
}

export enum ReseedStatus {
    InLine = 'in-line',
    InProgress = 'in-progress',
    Failed = 'failed',
}

export const updateReseedJobStatusById = async (id: number, newStatus: ReseedStatus) => {
    await reseedQueue().where({ id }).update({ status: newStatus })
}

export const deleteReseedJobById = async (id: number) => {
    await reseedQueue().where({ id }).delete()
}

// Just delete successful seed cursors.
export async function reseedSucceeded(ids: number | string | (string | number)[]) {
    ids = Array.isArray(ids) ? ids : [ids]
    if (!ids.length) return
    try {
        await db.transaction(async (tx) => {
            await reseedQueue(tx)
                .whereIn('id', unique(ids as any[]))
                .del()
        })
    } catch (err) {
        logger.error(`Error deleting reseed_queue upon success (ids=${ids.join(',')}): ${err}`)
    }
}
