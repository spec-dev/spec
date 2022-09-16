import constants from './constants'
import logdna from '@logdna/logger'

const dnaLogger = constants.LOGS_API_KEY 
    ? logdna.createLogger(constants.LOGS_API_KEY, {
        app: 'spec',
        level: 'info',
    })
    : null

class Logger {
    info(...args: any[]) {
        console.log(...args)
        dnaLogger?.log(args[0])
    }
    warn(...args: any[]) {
        console.warn(...args)
        dnaLogger?.warn(args[0])
    }
    error(...args: any[]) {
        console.error(...args)
        dnaLogger?.error(args[0])
    }
}

const logger: Logger = new Logger()
export default logger