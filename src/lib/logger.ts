import { createEventClient, SpecEventClient } from '@spec.dev/event-client'
import { Log, LogLevel } from './types'
import constants from './constants'
import chalk from 'chalk'

enum RPC {
    Ping = 'ping',
    Log = 'log',
}

export class Logger {
    
    client: SpecEventClient

    pingJob: any = null

    buffer: Log[] = []

    constructor() {
        this.client = createEventClient({
            hostname: constants.LOGS_HOSTNAME,
            port: constants.LOGS_PORT,
            signedAuthToken: constants.LOGS_API_KEY,
            ackTimeout: 30000,
            onConnect: () => {
                this._transmitBufferedLogs()
                this._createPingJobIfNotExists()
            },
        })
    }

    info(...args: any[]) {
        const log = this._newLog(args, LogLevel.Info)
        console.log(log.message)
        this._processLog(log)
    }

    warn(...args: any[]) {
        const log = this._newLog(args, LogLevel.Warn) 
        console.warn(log.message)
        this._processLog(log)
    }

    error(...args: any[]) {
        const log = this._newLog(args, LogLevel.Error)
        console.error(log.message)
        this._processLog(log)
    }

    _newLog(args: any[], level: LogLevel): Log {
        return {
            level,
            message: this._formatArgsAsMessage(args, level),
            timestamp: new Date(new Date().toUTCString()).toISOString(),
        }
    }

    _processLog(log: Log) {
        if (!this.client.isConnected) {
            this.buffer.push(log)
            return
        }
        this._transmitBufferedLogs()
        this._transmitLog(log)
    }

    _transmitLog(log: Log) {
        this.client.socket?.transmit(RPC.Log, log)
    }

    _transmitBufferedLogs() {
        while (this.buffer.length > 0) {
            const log = this.buffer.shift()
            this._transmitLog(log)
        }
    }

    _formatArgsAsMessage(args: any[], level: LogLevel): string {
        let message = ''
        try {
            message = args.join(' ')
        } catch (err) {
            message = (args[0] || '').toString()
        }

        if (level === LogLevel.Warn) {
            message = chalk.yellow(message)
        } else if (level === LogLevel.Error) {
            message = chalk.red(message)
        }

        return message
    }

    async invoke(functionName: RPC, payload?: any) {
        try {
            await this.client.socket?.invoke(functionName, payload)
        } catch (err) {
            this.error(err)
        }
    }

    _createPingJobIfNotExists() {
        this.pingJob = this.pingJob || setInterval(
            () => this.invoke(RPC.Ping, { ping: true }),
            constants.LOGS_PING_INTERVAL,
        )
    }
}

const logger = new Logger()
export default logger