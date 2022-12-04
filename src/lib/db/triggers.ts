import logger from '../logger'
import { db } from './index'
import { Trigger, TriggerEvent, StringKeyMap } from '../types'
import constants from '../constants'
import { tablesMeta } from './tablesMeta'
import { hash } from '../utils/hash'

// Trigger name components.
export const triggerName: StringKeyMap = {
    INSERT_PREFIX: 'spec_insert',
    UPDATE_PREFIX: 'spec_update',
    DELETE_PREFIX: 'spec_delete',
}

triggerName.prefixForEvent = (triggerEvent: TriggerEvent): string | null =>
    ({
        [TriggerEvent.INSERT]: triggerName.INSERT_PREFIX,
        [TriggerEvent.UPDATE]: triggerName.UPDATE_PREFIX,
        [TriggerEvent.DELETE]: triggerName.DELETE_PREFIX,
    }[triggerEvent] || null)

export function formatTriggerName(
    schema: string,
    table: string,
    event: TriggerEvent,
    primaryKeys?: string[]
): string {
    const prefix = triggerName.prefixForEvent(event)
    const suffix = (primaryKeys || []).sort().join('_')
    const main = `${schema}_${table}__pks__${suffix}`.toLowerCase()
    return prefix ? `${prefix}__${hash(main)}` : ''
}

export function formatRecordAsTrigger(record: StringKeyMap): Trigger {
    const { schema, table, event, trigger_name } = record
    return {
        schema,
        table: table.replace(/"/gi, ''),
        event: event as TriggerEvent,
        name: trigger_name,
    }
}

export async function getSpecTriggers(): Promise<Trigger[]> {
    const { rows } = await db.raw(
        `SELECT
            event_object_schema as schema,
            event_object_table as table,
            event_manipulation as event,
            trigger_name
        FROM information_schema.triggers
        WHERE 
            trigger_name LIKE '${triggerName.INSERT_PREFIX}%' OR 
            trigger_name LIKE '${triggerName.UPDATE_PREFIX}%'`
    )
    return (rows || []).map(formatRecordAsTrigger)
}

export async function createTrigger(schema: string, table: string, event: TriggerEvent) {
    const tablePath = [schema, table].join('.')
    const tableHasUppercaseLetters = table.match(/[A-Z]/g) !== null
    const officialTableName = tableHasUppercaseLetters ? `"${table}"` : table
    const officialTablePath = [schema, officialTableName].join('.')

    // Need primary keys of the table to serve as the suffix of the trigger name.
    const primaryKeys = tablesMeta[tablePath].primaryKey.map((pk) => pk.name)
    if (!primaryKeys.length) {
        throw `Can't create trigger -- no primary keys found for table ${tablePath}`
    }
    const primaryKeysAsArgs = primaryKeys.map((pk) => `'${pk}'`).join(', ')

    // Create the unique, formatted trigger name.
    const triggerName = formatTriggerName(schema, table, event, primaryKeys)
    if (!triggerName) throw 'Failed to create trigger - formatted name came back empty'

    logger.info(`Creating ${event} trigger ${triggerName}...`)

    await db.raw(
        `CREATE TRIGGER ${triggerName} AFTER ${event} ON ${officialTablePath}
        FOR EACH ROW EXECUTE PROCEDURE ${constants.TABLE_SUB_FUNCTION_NAME}(${primaryKeysAsArgs})`
    )
}

export async function dropTrigger(trigger: Trigger) {
    logger.info(`Dropping trigger ${trigger.name}...`)
    const tableHasUppercaseLetters = trigger.table.match(/[A-Z]/g) !== null
    const officialTableName = tableHasUppercaseLetters ? `"${trigger.table}"` : trigger.table
    const officialTablePath = [trigger.schema, officialTableName].join('.')
    await db.raw(`DROP TRIGGER ${trigger.name} ON ${officialTablePath}`)
}
