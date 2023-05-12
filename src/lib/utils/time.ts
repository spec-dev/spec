import { padDateNumber } from './formatters'

export async function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

export const subtractMinutes = (date: Date, minutes: number): Date => {
    const prevDate = new Date(date)
    prevDate.setMinutes(date.getMinutes() - minutes)
    return prevDate
}

export function formatPgDateString(d: Date, floorSeconds: boolean = true): string {
    const [year, month, date, hour, minutes] = [
        d.getUTCFullYear(),
        d.getUTCMonth() + 1,
        d.getUTCDate(),
        d.getUTCHours(),
        d.getUTCMinutes(),
    ]
    const seconds = floorSeconds ? '00' : padDateNumber(d.getUTCSeconds())
    const dateSection = [year, padDateNumber(month), padDateNumber(date)].join('-')
    const timeSection = [padDateNumber(hour), padDateNumber(minutes), seconds].join(':')
    return `${dateSection} ${timeSection}+00`
}