import { schema, db } from '..'
import { LinksTableRecord, StringKeyMap } from '../../types'
import { SPEC_SCHEMA_NAME, LINKS_TABLE_NAME } from './names'
import logger from '../../logger'
import { camelizeKeys, decamelizeKeys } from 'humps'

const links = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(LINKS_TABLE_NAME)

export async function getCachedLinks(primaryKeys: StringKeyMap[]): Promise<LinksTableRecord[]> {
    if (!primaryKeys.length) return []
    primaryKeys = decamelizeKeys(primaryKeys)

    let records
    try {
        const query = links().select('*')
        for (let i = 0; i < primaryKeys.length; i++) {
            const group = primaryKeys[i]
            if (i === 0) {
                query.where(group)
            } else {
                query.orWhere(group)
            }
        }
        records = await query
    } catch (err) {
        logger.error(
            `Error getting cached links for primary keys: ${JSON.stringify(primaryKeys)}: ${err}`
        )
        throw err
    }

    return camelizeKeys(records || []) as LinksTableRecord[]
}

export async function saveLinks(records: LinksTableRecord[]) {
    if (!records.length) return
    try {
        await db.transaction(async (tx) => {
            await links(tx)
                .insert(decamelizeKeys(records))
                .onConflict(['table_path', 'live_object_id'])
                .merge()
        })
    } catch (err) {
        logger.error(`Error saving links: ${err}`)
        throw err
    }
}
