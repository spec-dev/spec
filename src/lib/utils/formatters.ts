import { AnyMap } from '../types'

export const noop = () => {}

export function mapBy<T>(arr: T[], key: string): { [key: string]: T } {
    const m = {}
    for (let item of arr) {
        m[item[key]] = item
    }
    return m
}