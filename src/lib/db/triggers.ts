import logger from '../logger'
import { db } from './index'
import {
    Trigger,
    TriggerEvent,
    TriggerProcedure,
    StringKeyMap,
    StringMap,
    LiveObject,
} from '../types'
import { constants } from '../constants'
import { tablesMeta } from './tablesMeta'
import { hash } from '../utils/hash'
import { identPath } from '../utils/formatters'
import chalk from 'chalk'
import config from '../config'

// Trigger name components.
const triggerName: StringMap = {
    INSERT_PREFIX: 'spec_insert',
    UPDATE_PREFIX: 'spec_update',
    TRACK_OPS_INSERT_PREFIX: 'spec_track_ops_insert',
    TRACK_OPS_UPDATE_PREFIX: 'spec_track_ops_update',
}

const tableSubTriggerNamePrefixForEvent = {
    [TriggerEvent.INSERT]: triggerName.INSERT_PREFIX,
    [TriggerEvent.UPDATE]: triggerName.UPDATE_PREFIX,
}

const trackOpsTriggerNamePrefixForEvent = {
    [TriggerEvent.INSERT]: triggerName.TRACK_OPS_INSERT_PREFIX,
    [TriggerEvent.UPDATE]: triggerName.TRACK_OPS_UPDATE_PREFIX,
}

function formatRecordAsTrigger(record: StringKeyMap): Trigger {
    const { schema, table, event, trigger_name } = record
    return {
        schema,
        table: table.replace(/"/gi, ''),
        event: event as TriggerEvent,
        name: trigger_name,
    }
}

export function formatTriggerName(
    schema: string,
    table: string,
    event: TriggerEvent,
    procedure: TriggerProcedure,
    primaryKeys: string[]
): string {
    let prefix = ''
    switch (procedure) {
        case TriggerProcedure.TableSub:
            prefix = tableSubTriggerNamePrefixForEvent[event]
            break
        case TriggerProcedure.TrackOps:
            prefix = trackOpsTriggerNamePrefixForEvent[event]
            break
        default:
            return ''
    }
    const suffix = (primaryKeys || []).sort().join('_')
    const main = `${schema}_${table}__pks__${suffix}`.toLowerCase()
    return prefix ? `${prefix}__${hash(main).slice(0, 8)}` : ''
}

export async function getSpecTriggers(procedure: TriggerProcedure): Promise<Trigger[]> {
    let insertPrefix, updatePrefix
    switch (procedure) {
        case TriggerProcedure.TableSub:
            insertPrefix = triggerName.INSERT_PREFIX
            updatePrefix = triggerName.UPDATE_PREFIX
            break
        case TriggerProcedure.TrackOps:
            insertPrefix = triggerName.TRACK_OPS_INSERT_PREFIX
            updatePrefix = triggerName.TRACK_OPS_UPDATE_PREFIX
            break
        default:
            return []
    }
    const { rows } = await db.raw(
        `SELECT
            event_object_schema as schema,
            event_object_table as table,
            event_manipulation as event,
            trigger_name
        FROM information_schema.triggers
        WHERE 
            trigger_name LIKE '${insertPrefix}%' OR 
            trigger_name LIKE '${updatePrefix}%'`
    )
    return (rows || []).map(formatRecordAsTrigger)
}

export async function createTrigger(
    schema: string,
    table: string,
    event: TriggerEvent,
    procedure: TriggerProcedure,
    liveObjects?: { [key: string]: LiveObject }
) {
    const tablePath = [schema, table].join('.')

    // Need primary keys of the table to serve as the suffix of the trigger name.
    const primaryKeys = tablesMeta[tablePath].primaryKey.map((pk) => pk.name)
    if (!primaryKeys.length) {
        throw `Can't create trigger -- no primary keys found for table ${tablePath}`
    }

    // Create the unique, formatted trigger name.
    const triggerName = formatTriggerName(schema, table, event, procedure, primaryKeys)
    if (!triggerName) throw 'Failed to create trigger - formatted name came back empty'

    // Table-sub trigger.
    let procedureName, procedureArgs
    if (procedure === TriggerProcedure.TableSub) {
        procedureName = constants.TABLE_SUB_FUNCTION_NAME
        procedureArgs = primaryKeys
    }

    // Op-tracking trigger.
    else if (procedure === TriggerProcedure.TrackOps) {
        const tableChainInfo = config.getChainInfoForTable(tablePath, liveObjects)
        if (!tableChainInfo) throw `Invalid chain info for table: ${tablePath}`
        procedureName = constants.TRACK_OPS_FUNCTION_NAME
        procedureArgs = [
            tableChainInfo.defaultChainId || '',
            tableChainInfo.chainIdColName || '',
            tableChainInfo.blockNumberColName,
            ...primaryKeys,
        ]
    } else {
        throw `Unknown procedure ${procedure}`
    }

    logger.info(chalk.magenta(`Creating ${event} trigger ${triggerName} on ${tablePath}`))

    await db.raw(
        `CREATE TRIGGER ${triggerName} AFTER ${event} ON ${identPath(tablePath)}
        FOR EACH ROW EXECUTE PROCEDURE ${procedureName}(${procedureArgs
            .map((arg) => `'${arg}'`)
            .join(', ')})`
    )
}

export async function maybeDropTrigger(
    trigger: Trigger,
    schema: string,
    table: string,
    procedure: TriggerProcedure,
    primaryKeyColumnNames: string[]
): Promise<boolean> {
    // If the trigger name is different, it means the table's
    // primary keys must have changed, so drop the existing trigger.
    const expectedTriggerName = formatTriggerName(
        schema,
        table,
        trigger.event,
        procedure,
        primaryKeyColumnNames
    )
    if (expectedTriggerName && trigger.name !== expectedTriggerName) {
        try {
            // May fail unless spec is given enhanced permissions.
            await dropTrigger(trigger)
            return true
        } catch (err) {
            logger.error(`Error dropping trigger: ${trigger.name}`)
        }
    }

    return false
}

export async function dropTrigger(trigger: Trigger) {
    logger.info(chalk.magenta(`Dropping trigger ${trigger.name}...`))
    await db.raw(`DROP TRIGGER ${trigger.name} ON ${identPath(trigger.table)}`)
}
