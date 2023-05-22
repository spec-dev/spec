#!/usr/bin/env node
import { ArgumentParser } from 'argparse'
import { constants } from './lib/constants'
import path from 'path'

const parser = new ArgumentParser()
parser.add_argument('--config-dir')
parser.add_argument('--user')
parser.add_argument('--password')
parser.add_argument('--host')
parser.add_argument('--port')
parser.add_argument('--name')
parser.add_argument('--id')
parser.add_argument('--api-key')
parser.add_argument('--stream-logs')
parser.add_argument('--debug')

const parsed = parser.parse_args()
constants.SPEC_CONFIG_DIR = parsed.config_dir
    ? path.resolve(parsed.config_dir)
    : constants.SPEC_CONFIG_DIR
constants.PROJECT_CONFIG_PATH = path.join(
    constants.SPEC_CONFIG_DIR,
    constants.PROJECT_CONFIG_FILE_NAME
)
constants.DB_USER = parsed.user || constants.DB_USER
constants.DB_PASSWORD = parsed.password || constants.DB_PASSWORD
constants.DB_HOST = parsed.host || constants.DB_HOST
constants.DB_PORT = parsed.port ? Number(parsed.port) : constants.DB_PORT
constants.DB_NAME = parsed.name || constants.DB_NAME
constants.PROJECT_ID = parsed.id || constants.PROJECT_ID
constants.PROJECT_API_KEY = parsed.api_key || constants.PROJECT_API_KEY
constants.STREAM_LOGS = parsed.stream_logs || constants.STREAM_LOGS
constants.DEBUG = parsed.debug ? ['true', true].includes(parsed.debug) : constants.DEBUG

import Spec from './spec'
new Spec().start()
