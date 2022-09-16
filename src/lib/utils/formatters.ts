import { AnyMap } from '../types'

export const noop = () => {}

export function mapBy<T>(arr: T[], key: string): { [key: string]: T } {
    const m = {}
    for (let item of arr) {
        m[item[key]] = item
    }
    return m
}

export function reverseMap(obj: AnyMap): AnyMap {
    const reverse = {}
    for (let key in obj) {
        reverse[obj[key]] = key
    }
    return reverse
}

export function toMap(obj): AnyMap {
    const newObj = {}
    for (let key in obj) {
        newObj[key] = obj[key]
    }
    return newObj
}

export function unique(arr: any[]): any[] {
    return Array.from(new Set(arr))
}

export const fromNamespacedVersion = (
    namespacedVersion: string
): { nsp: string; name: string; version: string } => {
    const atSplit = (namespacedVersion || '').split('@')
    if (atSplit.length !== 2) {
        return { nsp: '', name: '', version: '' }
    }
    const [nspName, version] = atSplit
    const dotSplit = (nspName || '').split('.')
    if (dotSplit.length !== 2) {
        return { nsp: '', name: '', version: '' }
    }
    const [nsp, name] = dotSplit
    return { nsp, name, version }
}
