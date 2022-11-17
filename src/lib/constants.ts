import { ev } from './utils/env'
import path from 'path'
import { StringKeyMap } from './types'

const constants: StringKeyMap = {
    // TODO
    SPEC_CONFIG_DIR: path.resolve(ev('SPEC_CONFIG_DIR', '.spec')),
    PROJECT_CONFIG_FILE_NAME: ev('PROJECT_CONFIG_FILE_NAME', 'project.toml'),

    // Project credentials.
    PROJECT_ID: ev('PROJECT_ID'),
    PROJECT_API_KEY: ev('PROJECT_API_KEY'),
    PROJECT_ADMIN_KEY: ev('PROJECT_ADMIN_KEY'),

    // TODO
    EVENTS_HOSTNAME: ev('EVENTS_HOSTNAME', 'events.spec.dev'),
    EVENTS_PORT: Number(ev('EVENTS_PORT', 443)),
    SEEN_EVENTS_CACHE_SIZE: Number(ev('SEEN_EVENTS_CACHE_SIZE', 1000)),
    EVENTS_PING_INTERVAL: Number(ev('SEEN_EVENTS_CACHE_SIZE', 30000)),

    // TODO
    DB_HOST: ev('DB_HOST', 'localhost'),
    DB_PORT: Number(ev('DB_PORT', 5432)),
    DB_USER: ev('DB_USER', 'spec'),
    DB_PASSWORD: ev('DB_PASSWORD'),
    DB_NAME: ev('DB_NAME'),
    MAX_POOL_SIZE: Number(ev('MAX_POOL_SIZE', 100)),

    // TODO
    SEED_INPUT_BATCH_SIZE: Number(ev('SEED_INPUT_BATCH_SIZE', 1000)),
    FOREIGN_SEED_INPUT_BATCH_SIZE: Number(ev('SEED_INPUT_BATCH_SIZE', 10)),
    STREAMING_SEED_UPSERT_BATCH_SIZE: Number(ev('STREAMING_SEED_UPSERT_BATCH_SIZE', 1000)),

    // TODO
    DEBUG: ['true', true].includes(ev('DEBUG')),

    // TODO:
    SAVE_EVENT_CURSORS_INTERVAL: Number(ev('SAVE_EVENT_CURSORS_INTERVAL', 1000)),
    ANALYZE_TABLES_INTERVAL: Number(ev('ANALYZE_TABLES_INTERVAL', 30000)),
    RETRY_SEED_CURSORS_INTERVAL: Number(ev('RETRY_SEED_CURSORS_INTERVAL', 30000)),

    TABLE_SUB_FUNCTION_NAME: ev('TABLE_SUB_FUNCTION_NAME', 'spec_table_sub'),
    TABLE_SUB_CHANNEL: ev('TABLE_SUB_CHANNEL', 'spec:data-change'),
    TABLE_SUB_BUFFER_INTERVAL: Number(ev('TABLE_SUB_BUFFER_INTERVAL', 100)),
    TABLE_SUB_BUFFER_MAX_SIZE: Number(ev('TABLE_SUB_BUFFER_MAX_SIZE', 1000)),
    TABLE_SUB_UPDATED_AT_COL_NAME: ev('TABLE_SUB_UPDATED_AT_COL_NAME', 'updated_at'),

    MAX_UPDATES_BEFORE_BULK_UPDATE_USED: Number(ev('MAX_UPDATES_BEFORE_BULK_UPDATE_USED', 10)),

    LOGS_HOSTNAME: ev('LOGS_HOSTNAME', 'logs.spec.dev'),
    LOGS_PORT: Number(ev('LOGS_PORT', 443)),
    STREAM_LOGS: !['false', false].includes(ev('STREAM_LOGS')),
}

constants.PROJECT_CONFIG_PATH = path.join(
    constants.SPEC_CONFIG_DIR,
    constants.PROJECT_CONFIG_FILE_NAME
)

export default constants
