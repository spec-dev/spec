import { ev } from './utils/env'
import path from 'path'
import { StringKeyMap } from './types'

const constants: StringKeyMap = {
    // TODO
    SPEC_CONFIG_DIR: path.resolve(ev('SPEC_CONFIG_DIR', '/app/.spec')),

    // TODO
    EVENTS_HOSTNAME: ev('EVENTS_HOSTNAME', 'events.spec.dev'),
    EVENTS_PORT: ev('EVENTS_PORT', 8888),

    FUNCTIONS_ORIGIN: ev('FUNCTIONS_ORIGIN', 'https://functions.spec.dev'),
    SEED_BATCH_SIZE: 50,

    // TODO
    DB_HOST: ev('DB_HOST', 'localhost'),
    DB_PORT: ev('DB_PORT', 5432),
    DB_USER: ev('DB_USER', 'spec'),
    DB_PASSWORD: ev('DB_PASSWORD'),
    DB_NAME: ev('DB_NAME'),
    
    // TODO
    DEBUG: ['true', true].includes(ev('DEBUG')),

    // TODO: 
    SAVE_EVENT_CURSORS_INTERVAL: 1000,

    TABLE_SUBS_CHANNEL: 'spec_data_change_notifications'
}

constants.PROJECT_CONFIG_PATH = path.join(constants.SPEC_CONFIG_DIR, 'project.toml')

export default constants