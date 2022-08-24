import { ev } from './utils/env'
import path from 'path'
import { StringKeyMap } from './types'

const constants: StringKeyMap = {
    // TODO
    SPEC_CONFIG_DIR: path.resolve(ev('SPEC_CONFIG_DIR', '/app/.spec')),

    // TODO
    EVENTS_HOSTNAME: ev('EVENTS_HOSTNAME', 'events.spec.dev'),
    EVENTS_PORT: Number(ev('EVENTS_PORT', 8888)),

    // TODO
    DB_HOST: ev('DB_HOST', 'localhost'),
    DB_PORT: Number(ev('DB_PORT', 5432)),
    DB_USER: ev('DB_USER', 'spec'),
    DB_PASSWORD: ev('DB_PASSWORD'),
    DB_NAME: ev('DB_NAME'),

    // TODO
    SEED_INPUT_BATCH_SIZE: Number(ev('SEED_INPUT_BATCH_SIZE', 1000)),
    STREAMING_SEED_UPSERT_BATCH_SIZE: Number(ev('STREAMING_SEED_UPSERT_BATCH_SIZE', 1000)),
    
    // TODO
    DEBUG: ['true', true].includes(ev('DEBUG')),

    // TODO: 
    SAVE_EVENT_CURSORS_INTERVAL: 1000,

    TABLE_SUBS_CHANNEL: 'spec_data_change_notifications',
    TABLE_SUB_BUFFER_INTERVAL: 1000,
    TABLE_SUB_BUFFER_MAX_SIZE: 1000,
}

constants.PROJECT_CONFIG_PATH = path.join(constants.SPEC_CONFIG_DIR, 'project.toml')

export default constants