import { schema, db } from '..'
import { SeedCursor, SeedCursorStatus, StringKeyMap } from '../../types'
import { SPEC_SCHEMA_NAME, SEED_CURSORS_TABLE_NAME } from './names'
import logger from '../../logger'
import { unique } from '../../utils/formatters'
import { camelizeKeys, decamelizeKeys } from 'humps'
import { constants } from '../../constants'
import chalk from 'chalk'

export const seedCursors = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(SEED_CURSORS_TABLE_NAME)

export async function getSeedCursorWaitingInLine(id: string): Promise<SeedCursor | null> {
    let records
    try {
        records = await seedCursors()
            .select('*')
            .where({ id, status: SeedCursorStatus.InLine })
            .limit(1)
    } catch (err) {
        logger.error(`Error getting seed_cursor where id=${id}: ${err}`)
        return null
    }
    return camelizeKeys(records || [])[0] as SeedCursor
}

export async function getSeedCursorsWithStatus(
    status: SeedCursorStatus | SeedCursorStatus[]
): Promise<SeedCursor[]> {
    status = Array.isArray(status) ? status : [status]
    if (!status.length) return []
    let results
    try {
        results = await seedCursors().select('*').whereIn('status', unique(status))
    } catch (err) {
        logger.error(`Error getting seed_cursors for status: ${status.join(', ')}: ${err}`)
        return []
    }
    const records = camelizeKeys(results || []) as SeedCursor[]
    const finalRecords = []
    for (const record of records) {
        if (
            record.status === SeedCursorStatus.Failed &&
            !!record.metadata?.attempts &&
            record.metadata.attempts > constants.MAX_SEED_JOB_ATTEMPTS
        ) {
            continue
        }
        finalRecords.push(record)
    }
    return finalRecords
}

export async function createSeedCursor(seedCursor: StringKeyMap) {
    try {
        await db.transaction(async (tx) => {
            await seedCursors(tx).insert({
                ...decamelizeKeys(seedCursor),
                created_at: db.raw(`CURRENT_TIMESTAMP at time zone 'UTC'`),
            })
        })
    } catch (err) {
        const { liveObjectId, tablePath } = seedCursor.spec
        logger.error(
            `Error creating seed_cursor $(liveObjectId=${liveObjectId}, tablePath=${tablePath}): ${err}`
        )
    }
}

export async function processSeedCursorBatch(
    inserts: StringKeyMap[],
    updateToInProgressIds: string[] = [],
    deleteIds: string[] = []
): Promise<boolean> {
    if (!inserts.length && !updateToInProgressIds.length && !deleteIds.length) {
        return true
    }

    try {
        await db.transaction(async (tx) => {
            let promises = []
            // Inserts.
            if (inserts.length) {
                promises.push(
                    seedCursors(tx).insert(
                        inserts.map((seedCursor) => ({
                            ...decamelizeKeys(seedCursor),
                            created_at: db.raw(`CURRENT_TIMESTAMP at time zone 'UTC'`),
                        }))
                    )
                )
            }
            // Updates.
            if (updateToInProgressIds.length) {
                promises.push(
                    seedCursors(tx)
                        .update('status', SeedCursorStatus.InProgress)
                        .whereIn('id', unique(updateToInProgressIds as any[]))
                )
            }
            // Deletes.
            if (deleteIds.length) {
                seedCursors(tx)
                    .whereIn('id', unique(deleteIds as any[]))
                    .del()
            }
            await Promise.all(promises)
        })
    } catch (err) {
        logger.error(`Error processing seed_cursor batch: ${err}`)
        return false
    }
    return true
}

export async function seedFailed(id: string) {
    let results = []
    try {
        results = await seedCursors().select('metadata').where('id', id).limit(1)
    } catch (err) {
        logger.error(`Error getting metadata for seed_cursor (id=${id}): ${err}`)
    }
    const seedCursor = (results || [])[0] || {}
    const metadata = seedCursor.metadata || {}
    metadata.attempts = (metadata.attempts || 1) + 1

    if (metadata.attempts > constants.MAX_SEED_JOB_ATTEMPTS) {
        const tablePath = seedCursor.spec?.tablePath
        logger.error(
            chalk.red(`Max attempts hit while seeding ${tablePath} (seed_cursor.id = ${id})`)
        )
    }

    try {
        await db.transaction(async (tx) => {
            await seedCursors(tx)
                .update('metadata', metadata)
                .update('status', SeedCursorStatus.Failed)
                .where('id', id)
        })
    } catch (err) {
        logger.error(
            `Error setting seed_cursor (id=${id}) to failed (attempts=${metadata.attempts}): ${err}`
        )
    }
}

// Just delete successful seed cursors.
export async function seedSucceeded(ids: string | string[]) {
    ids = Array.isArray(ids) ? ids : [ids]
    if (!ids.length) return
    try {
        await db.transaction(async (tx) => {
            await seedCursors(tx)
                .whereIn('id', unique(ids as any[]))
                .del()
        })
    } catch (err) {
        logger.error(`Error deleting seed_cursors upon success (ids=${ids.join(',')}): ${err}`)
    }
}

export async function updateStatus(ids: string | string[], seedStatus: SeedCursorStatus) {
    ids = Array.isArray(ids) ? ids : [ids]
    if (!ids.length) return
    try {
        await db.transaction(async (tx) => {
            await seedCursors(tx)
                .update('status', seedStatus)
                .whereIn('id', unique(ids as any[]))
        })
    } catch (err) {
        logger.error(
            `Error updating seed_cursors (ids=${ids.join(',')}) to status ${seedStatus}: ${err}`
        )
    }
}

export async function updateCursor(id: string, cursor: number) {
    try {
        await db.transaction(async (tx) => {
            await seedCursors(tx).update('cursor', cursor).where('id', id)
        })
    } catch (err) {
        logger.error(`Error updating seed_cursor (id=${id}) to cursor ${cursor}: ${err}`)
    }
}

export async function failedSeedCursorsExist(): Promise<boolean> {
    try {
        const failed = (
            (await seedCursors().where('status', SeedCursorStatus.Failed)) || []
        ).filter(
            (r) => !r.metadata?.attempts || r.metadata.attempts <= constants.MAX_SEED_JOB_ATTEMPTS
        )
        return failed.length > 0
    } catch (err) {
        logger.error(`Error querying for failed seed jobs: ${err}`)
        return false
    }
}
