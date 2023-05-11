import path from 'path'
import { ev } from './utils/env'
import { StringKeyMap } from './types'
import os from 'os'

/**
 * Spec Environment Variables & Config.
 */
export const constants: StringKeyMap = {
    // Spec config file locations.
    SPEC_CONFIG_DIR: path.resolve(ev('SPEC_CONFIG_DIR', '.spec')),
    PROJECT_CONFIG_FILE_NAME: ev('PROJECT_CONFIG_FILE_NAME', 'project.toml'),
    SPEC_GLOBAL_DIR: path.join(os.homedir(), '.spec'),

    // Main database connection.
    DB_HOST: ev('DB_HOST', 'localhost'),
    DB_PORT: Number(ev('DB_PORT', 5432)),
    DB_USER: ev('DB_USER', 'spec'),
    DB_PASSWORD: ev('DB_PASSWORD', ''),
    DB_NAME: ev('DB_NAME'),
    DB_SSL: [true, 'true'].includes(ev('DB_SSL')),
    MAX_POOL_SIZE: Number(ev('MAX_POOL_SIZE', 100)),

    // Spec project credentials.
    PROJECT_ID: ev('PROJECT_ID'),
    PROJECT_API_KEY: ev('PROJECT_API_KEY'),
    PROJECT_ADMIN_KEY: ev('PROJECT_ADMIN_KEY'),

    // Spec Tables API (for backfills).
    SHARED_TABLES_ORIGIN: ev('SHARED_TABLES_ORIGIN', 'https://tables-ingress.spec.dev'),
    SHARED_TABLES_INITIAL_REQUEST_TIMEOUT: Number(
        ev('SHARED_TABLES_INITIAL_REQUEST_TIMEOUT', 60000)
    ),
    SHARED_TABLES_READABLE_STREAM_TIMEOUT: Number(
        ev('SHARED_TABLES_READABLE_STREAM_TIMEOUT', 60000)
    ),
    SHARED_TABLES_AUTH_HEADER_NAME: 'Spec-Auth-Token',

    // Spec Events API (for updates and new data).
    EVENTS_HOSTNAME: ev('EVENTS_HOSTNAME', 'events.spec.dev'),
    EVENTS_PORT: Number(ev('EVENTS_PORT', 443)),
    SEEN_EVENTS_CACHE_SIZE: Number(ev('SEEN_EVENTS_CACHE_SIZE', 1000)),
    EVENTS_PING_INTERVAL: Number(ev('SEEN_EVENTS_CACHE_SIZE', 30000)),

    // The number of records to use in a single input batch when
    // seeding live columns by an *adjacent column of the same table*.
    SEED_INPUT_BATCH_SIZE: Number(ev('SEED_INPUT_BATCH_SIZE', 1000)),

    // The number of records to use in a single batch when seeding live
    // columns with a *foreign table*.
    FOREIGN_SEED_INPUT_BATCH_SIZE: Number(ev('FOREIGN_SEED_INPUT_BATCH_SIZE', 100)),

    // The 'limit' to use (alongside 'offset') when fetching an entire live table from scratch.
    FROM_SCRATCH_SEED_INPUT_BATCH_SIZE: Number(ev('FROM_SCRATCH_SEED_INPUT_BATCH_SIZE', 100000)),

    // Batch size to upsert with once a new batch of streaming query request data is available.
    STREAMING_SEED_UPSERT_BATCH_SIZE: Number(ev('STREAMING_SEED_UPSERT_BATCH_SIZE', 1000)),

    // How often to save the most recent events recieved from the events API to the DB.
    SAVE_EVENT_CURSORS_INTERVAL: Number(ev('SAVE_EVENT_CURSORS_INTERVAL', 1000)),

    // How often to poll the database schema for any changes.
    ANALYZE_TABLES_INTERVAL: Number(ev('ANALYZE_TABLES_INTERVAL', 30000)),

    // When a seed/backfill fails, the interval to wait before retrying
    // and the max number of times to retry.
    RETRY_SEED_CURSORS_INTERVAL: Number(ev('RETRY_SEED_CURSORS_INTERVAL', 5000)),
    MAX_SEED_JOB_ATTEMPTS: Number(ev('MAX_SEED_JOB_ATTEMPTS', 10)),

    // The LISTEN/NOTIFY channel and function name associated with
    // the triggers Spec uses to listen to table activity.
    TABLE_SUB_FUNCTION_NAME: 'spec_table_sub',
    TABLE_SUB_CHANNEL: 'spec_data_change',

    // Buffer config to use when debouncing table subscription events from postgres triggers.
    TABLE_SUB_BUFFER_INTERVAL: Number(ev('TABLE_SUB_BUFFER_INTERVAL', 100)),
    TABLE_SUB_BUFFER_MAX_SIZE: Number(ev('TABLE_SUB_BUFFER_MAX_SIZE', 1000)),

    // The column name to use to track the last time any table was updated (if it exists).
    TABLE_SUB_UPDATED_AT_COL_NAME: ev('TABLE_SUB_UPDATED_AT_COL_NAME', 'updated_at'),

    // Postgres trigger function name for tracking record operations. 
    TRACK_OPS_FUNCTION_NAME: 'spec_track_ops',

    // Batch size to use when rolling back records to a previous state.
    ROLLBACK_BATCH_SIZE: Number(ev('ROLLBACK_BATCH_SIZE', 2000)),

    // Threshold required to switch from individual update operations to a bulk update operation.
    MAX_UPDATES_BEFORE_BULK_UPDATE_USED: Number(ev('MAX_UPDATES_BEFORE_BULK_UPDATE_USED', 10)),

    // Max number of attempts when retrying a query that hits deadlock.
    MAX_DEADLOCK_RETRIES: Number(ev('MAX_DEADLOCK_RETRIES', 10)),

    // Whether to run in debug mode.
    DEBUG: ['true', true].includes(ev('DEBUG')),

    // Spec's log relay to stream logs into so that they
    // can easily be tailed via the `$ spec logs` command.
    LOGS_HOSTNAME: ev('LOGS_HOSTNAME', 'logs.spec.dev'),
    LOGS_PORT: Number(ev('LOGS_PORT', 443)),
    STREAM_LOGS: ev('STREAM_LOGS'),
    LOGS_ENV: ev('LOGS_ENV', 'prod'),

    // Exponential backoff config for certain retries.
    EXPO_BACKOFF_DELAY: 100,
    EXPO_BACKOFF_MAX_ATTEMPTS: 10,
    EXPO_BACKOFF_FACTOR: 1.5,

    // Special live object properties.
    CHAIN_ID_PROPERTY: 'chainId',
    BLOCK_NUMBER_PROPERTY: 'blockNumber', 
}

constants.PROJECT_CONFIG_PATH = path.join(
    constants.SPEC_CONFIG_DIR,
    constants.PROJECT_CONFIG_FILE_NAME
)
