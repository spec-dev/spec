import { schema, db } from '..'
import { SeedCursor, SeedCursorStatus, StringKeyMap } from '../../types'
import { SPEC_SCHEMA_NAME, SEED_CURSORS_TABLE_NAME } from './names'
import logger from '../../logger'
import { unique } from '../../utils/formatters'
import { camelizeKeys, decamelize } from 'humps'

export const seedCursors = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(SEED_CURSORS_TABLE_NAME)

export async function getSeedCursorsWithStatus(status: SeedCursorStatus | SeedCursorStatus[]): Promise<SeedCursor[]> {
    status = Array.isArray(status) ? status : [status]

    let records
    try {
        records = await seedCursors()
            .select('*')
            .whereIn('status', unique(status))
    } catch (err) {
        logger.error(`Error getting seed_cursors for status: ${status.join(', ')}: ${err}`)
        return []
    }

    return camelizeKeys(records || []) as SeedCursor[]
}

export async function createSeedCursor(seedCursor: StringKeyMap) {
    try {
        await db.transaction(async tx => {
            await seedCursors(tx)
                .insert({
                    ...decamelize(seedCursor),
                    created_at: db.raw(`CURRENT_TIMESTAMP at time zone 'UTC'`),
                })
        })
    } catch (err) {
        const { liveObjectId, tablePath } = seedCursor.spec
        logger.error(`Error creating seed_cursor $(liveObjectId=${liveObjectId}, tablePath=${tablePath}): ${err}`)
    }
}

export async function processSeedCursorBatch(
    inserts: StringKeyMap[], 
    updateToInProgressIds: string[] = [],
    deleteIds: string[] = []): Promise<boolean> {
    try {
        await db.transaction(async tx => {
            let promises = [
                seedCursors(tx)
                    .insert(inserts.map(seedCursor => ({
                        ...decamelize(seedCursor),
                        created_at: db.raw(`CURRENT_TIMESTAMP at time zone 'UTC'`),    
                    })))
            ]

            if (updateToInProgressIds.length) {
                promises.push(
                    seedCursors(tx)
                        .update('status', SeedCursorStatus.InProgress)
                        .whereIn('id', unique(updateToInProgressIds as any[]))
                )
            }
            
            if (deleteIds.length) {
                seedCursors(tx).whereIn('id', unique(deleteIds as any[])).del()
            }

            await Promise.all(promises)
        })
    } catch (err) {
        logger.error(`Error processing seed_cursor batch: ${err}`)
        return false
    }
    return true
}

export async function seedFailed(ids: string | string[]) {
    await updateStatus(ids, SeedCursorStatus.Failed)
}

export async function seedSucceeded(ids: string | string[]) {
    await updateStatus(ids, SeedCursorStatus.Succeeded)
}

export async function updateStatus(ids: string | string[], seedStatus: SeedCursorStatus) {
    ids = Array.isArray(ids) ? ids : [ids]
    try {
        await db.transaction(async tx => {
            await seedCursors(tx)
                .update('status', seedStatus)
                .whereIn('id', unique(ids as any[]))
        })
    } catch (err) {
        logger.error(`Error updating seed_cursors (ids=${ids.join(',')}) to status ${seedStatus}: ${err}`)
    }
}

export async function updateCursor(id: string, cursor: number) {
    try {
        await db.transaction(async tx => {
            await seedCursors(tx).update('cursor', cursor).where('id', id)
        })
    } catch (err) {
        logger.error(`Error updating seed_cursor (id=${id}) to cursor ${cursor}: ${err}`)
    }
}