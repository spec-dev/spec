import { AnyMap, StringKeyMap } from '../types'

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

export function fromNamespacedVersion(
    namespacedVersion: string
): { nsp: string; name: string; version: string } {
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

export function getCombinations(values: any[]) {
    return cartesian(values.map(v => Array.isArray(v) ? v : [v]))
}

export function cartesian(args: any[]) {
    var r = []
    var max = args.length - 1
    function helper(arr, i) {
        for (var j = 0, l = args[i].length; j < l; j++) {
            var a = arr.slice(0)
            a.push(args[i][j])
            if (i == max) {
                r.push(a)
            } else {
                helper(a, i + 1)
            }
        }
    }
    helper([], 0)
    return r
}

export const mergeByKeys = (iterable: StringKeyMap[], keys: string[]): StringKeyMap[] => {
    const m = {}
    for (let i = 0; i < iterable.length; i++) {
        const obj = iterable[i]
        const uniqueKeyId = keys.map((key) => obj[key] || '').join('__')
        
        if (!m.hasOwnProperty(uniqueKeyId)) {
            m[uniqueKeyId] = obj
            continue
        }
        
        const combinedObj = m[uniqueKeyId] || {}
        for (const key in obj) {
            if (keys.includes(key)) continue
            combinedObj[key] = obj[key]
        }
        m[uniqueKeyId] = combinedObj
    }
    return Object.values(m)
}

export function groupByKeys(input: StringKeyMap | StringKeyMap[]): StringKeyMap {
    const inputs = Array.isArray(input) ? input : [input]
    let groupedInputs = {}
    for (const entry of inputs) {
        for (const key in entry) {
            groupedInputs[key] = groupedInputs[key] || []
            const val = entry[key]
            if (Array.isArray(val)) {
                groupedInputs[key].push(...val)
            } else {
                groupedInputs[key].push(val)
            }
        }
    }
    for (const key in groupedInputs) {
        groupedInputs[key] = Array.from(new Set(groupedInputs[key]))
    }
    return groupedInputs
}