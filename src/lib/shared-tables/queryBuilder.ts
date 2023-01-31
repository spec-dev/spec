import { SharedTablesQueryPayload, StringKeyMap, FilterOp } from '../types'
import { decamelize } from 'humps'

const filterOpValues = new Set(Object.values(FilterOp))

/**
 * Create a sql query with bindings for the given table & filters.
 */
export function buildQuery(table: string, filters: StringKeyMap | StringKeyMap[]): SharedTablesQueryPayload {
    // Build initial select query.
    const select = `select * from ${formatRelation(table)}`

    // Type-check filters and handle case where empty.
    const filtersIsArray = Array.isArray(filters)
    const filtersIsObject = !filtersIsArray && typeof filters === 'object'
    if (
        !filters ||
        (filtersIsArray && !filters.length) ||
        (filtersIsObject && !Object.keys(filters).length)
    ) {
        return { sql: select, bindings: [] }
    }

    // Go ahead and group filters into an array for processing, even if not one.
    if (!filtersIsArray) {
        filters = [filters]
    }

    const orStatements: string[] = []
    const values: any = []
    const bindingIndex = { i: 1 }
    for (const inclusiveFilters of filters as StringKeyMap[]) {
        const andStatement = buildAndStatement(inclusiveFilters, values, bindingIndex)
        andStatement?.length && orStatements.push(andStatement)
    }
    if (!orStatements.length) {
        return { sql: select, bindings: [] }
    }

    const suffix =
        orStatements.length > 1 ? orStatements.map((s) => `(${s})`).join(' or ') : orStatements[0]

    return {
        sql: `${select} where ${suffix}`,
        bindings: values,
    }
}

export function buildAndStatement(
    filtersMap: StringKeyMap,
    values: any[],
    bindingIndex: StringKeyMap
) {
    if (!filtersMap) return null
    let numKeys
    try {
        numKeys = Object.keys(filtersMap).length
    } catch (e) {
        return null
    }
    if (!numKeys) return null

    const statements: string[] = []

    for (const key in filtersMap) {
        let value = filtersMap[key]
        const isArray = Array.isArray(value)
        const isObject = !isArray && typeof value === 'object'
        const isFilterObject = isObject && value.op && value.hasOwnProperty('value')

        if (
            value === null ||
            value === undefined ||
            (isArray && !value.length) ||
            (isArray && !!value.find((v) => Array.isArray(v))) ||
            (isObject && (!Object.keys(value).length || !isFilterObject))
        )
            continue

        let op
        if (isArray) {
            op = FilterOp.In
        } else if (isFilterObject) {
            op = value.op
            value = value.value
        } else {
            op = FilterOp.EqualTo
        }
        if (!filterOpValues.has(op)) continue

        let valuePlaceholder
        if (Array.isArray(value)) {
            const valuePlaceholders: string[] = []
            for (const v of value) {
                valuePlaceholders.push(`$${bindingIndex.i}`)
                values.push(v)
                bindingIndex.i++
            }
            valuePlaceholder = `(${valuePlaceholders.join(', ')})`
        } else {
            valuePlaceholder = `$${bindingIndex.i}`
            values.push(value)
            bindingIndex.i++
        }

        statements.push(`${formatRelation(key)} ${op} ${valuePlaceholder}`)
    }

    return statements.join(' and ')
}

function formatRelation(relation: string): string {
    return relation
        .split('.')
        .map((v) => `"${decamelize(v)}"`)
        .join('.')
}