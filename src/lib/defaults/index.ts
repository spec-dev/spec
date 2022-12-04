import { StringKeyMap, ColumnDefaultsConfig } from '../types'
import { uuid } from './identity'
import { updatedAtColNames, now } from './datetime'
export * from './datetime'
export * from './identity'

const defaultFunctions = {
    uuid: uuid,
    now: now,
}

function getDefault(value: any): any {
    if (typeof value !== 'string' || !value.endsWith('()') || value === '()') {
        return value
    }

    const defaultFunctionName = value.slice(0, value.length - 2)
    const defaultFunction = defaultFunctions[defaultFunctionName]
    if (!defaultFunction) return value

    return defaultFunction()
}

export function applyDefaults(
    data: StringKeyMap | StringKeyMap[],
    defaultColumnValues: { [key: string]: ColumnDefaultsConfig }
): StringKeyMap | StringKeyMap[] {
    if (Array.isArray(data)) {
        return data.map((d) => applyDefaults(d, defaultColumnValues))
    }
    for (const colName in defaultColumnValues) {
        if (!data.hasOwnProperty(colName) || updatedAtColNames.includes(colName)) {
            data[colName] = getDefault(defaultColumnValues[colName].value)
        }
    }
    return data
}
