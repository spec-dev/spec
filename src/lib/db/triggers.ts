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
    PK_SEP: '__pk__',
}

triggerName.prefixForEvent = (triggerEvent: TriggerEvent): string | null =>
    ({
        [TriggerEvent.INSERT]: triggerName.INSERT_PREFIX,
        [TriggerEvent.UPDATE]: triggerName.UPDATE_PREFIX,
    }[triggerEvent] || null)

// Function name components.
export const functionName: StringKeyMap = {
    INSERT_PREFIX: 'spec_insert_notify',
    UPDATE_PREFIX: 'spec_update_notify',
}

functionName.prefixForEvent = (triggerEvent: TriggerEvent): string | null =>
    ({
        [TriggerEvent.INSERT]: functionName.INSERT_PREFIX,
        [TriggerEvent.UPDATE]: functionName.UPDATE_PREFIX,
    }[triggerEvent] || null)

export function formatTriggerName(
    schema: string,
    table: string,
    event: TriggerEvent,
    primaryKeys?: string[]
): string {
    const suffix = (primaryKeys || []).sort().join('_')
    const prefix = triggerName.prefixForEvent(event)
    const main = `${schema}_${table}${triggerName.PK_SEP}${suffix}`.toLowerCase()
    return prefix ? `${prefix}__${hash(main)}` : ''
}

export function formatFunctionName(schema: string, table: string, event: TriggerEvent): string {
    const prefix = functionName.prefixForEvent(event)
    const main = `${schema}_${table}`.toLowerCase()
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

export async function createTrigger(
    schema: string,
    table: string,
    event: TriggerEvent,
    options: StringKeyMap = {}
) {
    const tablePath = [schema, table].join('.')
    const tableHasUppercaseLetters = table.match(/[A-Z]/g) !== null
    const officialTableName = tableHasUppercaseLetters ? `"${table}"` : table
    const officialTablePath = [schema, officialTableName].join('.')

    // Whether to also create the database function associated with the trigger.
    const withFunction = options.hasOwnProperty('withFunction') ? options.withFunction : true

    // Need primary keys of the table to serve as the suffix of the trigger name.
    const primaryKeys = tablesMeta[tablePath].primaryKey.map((pk) => pk.name)
    if (!primaryKeys.length) {
        throw `Can't create trigger -- no primary keys found for table ${tablePath}`
    }
    const primaryKeysAsArgs = primaryKeys.map((pk) => `'${pk}'`).join(', ')

    const triggerName = formatTriggerName(schema, table, event, primaryKeys)
    if (!triggerName) throw 'Failed to create trigger - formatted name came back empty'

    const functionName = formatFunctionName(schema, table, event)
    if (!functionName) throw 'Failed to create trigger -- formatted function name came back empty'

    switch (event) {
        case TriggerEvent.INSERT:
            withFunction && (await createInsertFunction(functionName, schema))
            logger.info(`Creating INSERT trigger ${schema}.${triggerName}...`)
            await db.raw(
                `CREATE TRIGGER ${triggerName} AFTER INSERT ON ${officialTablePath}
                FOR EACH ROW EXECUTE PROCEDURE ${schema}.${functionName}(${primaryKeysAsArgs})`
            )
            break

        case TriggerEvent.UPDATE:
            withFunction && (await createUpdateFunction(functionName, schema))
            logger.info(`Creating UPDATE trigger ${schema}.${triggerName}...`)
            await db.raw(
                `CREATE TRIGGER ${triggerName} AFTER UPDATE ON ${officialTablePath}
                FOR EACH ROW EXECUTE PROCEDURE ${schema}.${functionName}(${primaryKeysAsArgs})`
            )
            break

        default:
            break
    }
}

export async function dropTrigger(trigger: Trigger) {
    logger.info(`Dropping trigger ${trigger.name}...`)
    const tableHasUppercaseLetters = trigger.table.match(/[A-Z]/g) !== null
    const officialTableName = tableHasUppercaseLetters ? `"${trigger.table}"` : trigger.table
    const officialTablePath = [trigger.schema, officialTableName].join('.')
    await db.raw(`DROP TRIGGER ${trigger.name} ON ${officialTablePath}`)
}

export async function createInsertFunction(name: string, schema: string) {
    logger.info(`Creating database function ${name}...`)

    await db.raw(
        `CREATE OR REPLACE FUNCTION ${schema}.${name}() RETURNS trigger AS $$
        DECLARE
            rec RECORD;
            payload TEXT;
            column_name TEXT;
            column_value TEXT;
            primary_key_data TEXT[];
            col_names_with_values TEXT;
        BEGIN
            rec := NEW;

            FOREACH column_name IN ARRAY TG_ARGV LOOP
                EXECUTE format('SELECT $1.%I::TEXT', column_name)
                INTO column_value
                USING rec;
                primary_key_data := array_append(
                    primary_key_data, 
                    '"' || replace(column_name, '"', '\"') || '":"' || replace(column_value, '"', '\"') || '"'
                );
            END LOOP;
            
            col_names_with_values := (SELECT json_agg(key)::text
                FROM json_each_text(to_json(NEW)) n
                WHERE n.value IS NOT NULL);

            payload := ''
                || '{'
                || '"timestamp":"'         || CURRENT_TIMESTAMP at time zone 'UTC'   || '",'
                || '"operation":"'         || TG_OP                                  || '",'
                || '"schema":"'            || TG_TABLE_SCHEMA                        || '",'
                || '"table":"'             || TG_TABLE_NAME                          || '",'
                || '"primaryKeys":{'       || array_to_string(primary_key_data, ',') || '},'
                || '"colNamesWithValues":' || col_names_with_values                  || ''
                || '}';

            PERFORM pg_notify('${constants.TABLE_SUBS_CHANNEL}', payload);

            RETURN rec;
        END;
        $$ LANGUAGE plpgsql;`
    )
}

export async function createUpdateFunction(name: string, schema: string) {
    logger.info(`Creating database function ${name}...`)

    await db.raw(
        `CREATE OR REPLACE FUNCTION ${schema}.${name}() RETURNS trigger AS $$
        DECLARE
            rec RECORD;
            payload TEXT;
            column_name TEXT;
            column_value TEXT;
            primary_key_data TEXT[];
            col_names_changed TEXT;
        BEGIN
            rec := NEW;

            FOREACH column_name IN ARRAY TG_ARGV LOOP
                EXECUTE format('SELECT $1.%I::TEXT', column_name)
                INTO column_value
                USING rec;
                primary_key_data := array_append(
                    primary_key_data, 
                    '"' || replace(column_name, '"', '\"') || '":"' || replace(column_value, '"', '\"') || '"'
                );
            END LOOP;

            col_names_changed := (SELECT json_agg(key)::text
                FROM json_each_text(to_json(OLD)) o
                JOIN json_each_text(to_json(NEW)) n USING (key)
                WHERE n.value IS DISTINCT FROM o.value);
            
            payload := ''
                || '{'
                || '"timestamp":"'      || CURRENT_TIMESTAMP at time zone 'UTC'   || '",'
                || '"operation":"'      || TG_OP                                  || '",'
                || '"schema":"'         || TG_TABLE_SCHEMA                        || '",'
                || '"table":"'          || TG_TABLE_NAME                          || '",'
                || '"primaryKeys":{'    || array_to_string(primary_key_data, ',') || '},'
                || '"colNamesChanged":' || col_names_changed                      || ''
                || '}';

            PERFORM pg_notify('${constants.TABLE_SUBS_CHANNEL}', payload);

            RETURN rec;
        END;
        $$ LANGUAGE plpgsql;`
    )
}
