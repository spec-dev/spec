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

export function toChunks(arr: any[], chunkSize: number = 1): any[][] {
    const res = []
    for (let i = 0; i < arr.length; i += chunkSize) {
        const chunk = arr.slice(i, i + chunkSize)
        res.push(chunk)
    }
    return res
}