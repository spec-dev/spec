import constants from './constants'

class Logger {
    info(...args: any[]) {
        console.log(...args)
    }
    warn(...args: any[]) {
        console.warn(...args)
    }
    error(...args: any[]) {
        console.error(...args)
    }
}

const logger: Logger = new Logger()

export default logger
