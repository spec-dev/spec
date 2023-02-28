import dayjs from 'dayjs'
import utc from 'dayjs/plugin/utc'
import customParseFormat from 'dayjs/plugin/customParseFormat'
dayjs.extend(utc)
dayjs.extend(customParseFormat)

const specTimestampFilterFormat = 'YYYY-MM-DD HH:mm:ss'

export function toUTCDate(value: string): dayjs.Dayjs {
    return dayjs.utc(value)
}

export function isSpecTimestampFilterFormat(value: string): boolean {
    return dayjs(value, specTimestampFilterFormat, true).isValid()
}
