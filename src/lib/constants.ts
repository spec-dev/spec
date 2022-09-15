import { ev } from './utils/env'
import path from 'path'
import { StringKeyMap } from './types'

const constants: StringKeyMap = {
    // TODO
    SPEC_CONFIG_DIR: path.resolve(ev('SPEC_CONFIG_DIR', '.spec')),
    PROJECT_CONFIG_FILE_NAME: 'project.toml',

    // Project credentials.
    PROJECT_API_KEY: ev('PROJECT_API_KEY'),
    PROJECT_ADMIN_KEY: ev('PROJECT_ADMIN_KEY'),

    // TODO
    EVENTS_HOSTNAME: ev('EVENTS_HOSTNAME', 'events.spec.dev'),
    EVENTS_PORT: Number(ev('EVENTS_PORT', 443)),
    SEEN_EVENTS_CACHE_SIZE: Number(ev('SEEN_EVENTS_CACHE_SIZE', 1000)),
    EVENTS_PING_INTERVAL: 30000,

    // TODO
    DB_HOST: ev('DB_HOST', 'localhost'),
    DB_PORT: Number(ev('DB_PORT', 5432)),
    DB_USER: ev('DB_USER', 'spec'),
    DB_PASSWORD: ev('DB_PASSWORD'),
    DB_NAME: ev('DB_NAME'),
    MAX_POOL_SIZE: ev('MAX_POOL_SIZE', 50),

    // TODO
    SEED_INPUT_BATCH_SIZE: Number(ev('SEED_INPUT_BATCH_SIZE', 1000)),
    FOREIGN_SEED_INPUT_BATCH_SIZE: Number(ev('SEED_INPUT_BATCH_SIZE', 100)),
    STREAMING_SEED_UPSERT_BATCH_SIZE: Number(ev('STREAMING_SEED_UPSERT_BATCH_SIZE', 1000)),
    
    // TODO
    DEBUG: ['true', true].includes(ev('DEBUG')),

    // TODO: 
    SAVE_EVENT_CURSORS_INTERVAL: 1000,
    ANALYZE_TABLES_INTERVAL: 30000,
    RETRY_SEED_CURSORS_INTERVAL: 30000,

    TABLE_SUBS_CHANNEL: 'spec_data_change_notifications',
    TABLE_SUB_BUFFER_INTERVAL: 100,
    TABLE_SUB_BUFFER_MAX_SIZE: 1000,
    TABLE_SUB_UPDATED_AT_COL_NAME: 'updated_at',

    MAX_UPDATES_BEFORE_BULK_UPDATE_USED: 10,
}

constants.PROJECT_CONFIG_PATH = path.join(
    constants.SPEC_CONFIG_DIR, 
    constants.PROJECT_CONFIG_FILE_NAME,
)

export default constants