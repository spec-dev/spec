import { createEventClient, SpecEventClient } from '@spec.dev/event-client'
import { Log, LogLevel } from './types'
import { constants } from './constants'
import { fileExists, createDir } from './utils/file'
import path from 'path'
import fs from 'fs'

enum RPC {
    Ping = 'ping',
    Log = 'log',
}

const writeToLocalLogs = constants.STREAM_LOGS === 'local'

export class Logger {
    client: SpecEventClient | null

    pingJob: any = null

    buffer: Log[] = []

    localWriteStream: any = null

    constructor() {
        this.client = ['true', true].includes(constants.STREAM_LOGS)
            ? createEventClient({
                  hostname: constants.LOGS_HOSTNAME,
                  port: constants.LOGS_PORT,
                  signedAuthToken: constants.PROJECT_ADMIN_KEY,
                  ackTimeout: 30000,
                  onConnect: () => {
                      this._transmitBufferedLogs()
                      this._createPingJobIfNotExists()
                  },
              })
            : null

        if (writeToLocalLogs) {
            const localLogPath = path.join(constants.SPEC_GLOBAL_DIR, `${constants.PROJECT_ID}.log`)
            fileExists(constants.SPEC_GLOBAL_DIR) || createDir(constants.SPEC_GLOBAL_DIR)
            fs.openSync(localLogPath, 'w')
            this.localWriteStream = fs.createWriteStream(localLogPath, {
                flags: 'a',
            })
        }
    }

    info(...args: any[]) {
        const log = this._newLog(args, LogLevel.Info)
        console.log(log.message)
        this.client && this._processLog(log)
        this.localWriteStream?.write(`${log.message}\n`)
    }

    warn(...args: any[]) {
        const log = this._newLog(args, LogLevel.Warn)
        console.warn(log.message)
        this.client && this._processLog(log)
        this.localWriteStream?.write(`${log.message}\n`)
    }

    error(...args: any[]) {
        const log = this._newLog(args, LogLevel.Error)
        console.error(log.message)
        this.client && this._processLog(log)
        this.localWriteStream?.write(`${log.message}\n`)
    }

    _newLog(args: any[], level: LogLevel): Log {
        return {
            level,
            message: this._formatArgsAsMessage(args),
            timestamp: new Date(new Date().toUTCString()).toISOString(),
            projectId: constants.PROJECT_ID,
            env: constants.LOGS_ENV,
        }
    }

    _processLog(log: Log) {
        if (!this.client?.isConnected) {
            this.buffer.push(log)
            return
        }
        this._transmitBufferedLogs()
        this._transmitLog(log)
    }

    _transmitLog(log: Log) {
        this.client?.socket?.transmit(RPC.Log, log)
    }

    _transmitBufferedLogs() {
        while (this.buffer.length > 0) {
            const log = this.buffer.shift()
            this._transmitLog(log)
        }
    }

    _formatArgsAsMessage(args: any[]): string {
        let message = ''
        try {
            message = args.join(' ')
        } catch (err) {
            message = (args[0] || '').toString()
        }
        return message
    }

    async invoke(functionName: RPC, payload?: any) {
        try {
            await this.client?.socket?.invoke(functionName, payload)
        } catch (err) {}
    }

    _createPingJobIfNotExists() {
        this.pingJob =
            this.pingJob ||
            setInterval(() => this.invoke(RPC.Ping, { ping: true }), constants.LOGS_PING_INTERVAL)
    }
}

const logger = new Logger()
export default logger
