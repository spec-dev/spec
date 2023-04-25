import {
    ConstraintType,
    Constraint,
    ForeignKeyConstraint,
    StringKeyMap,
    DBColumn,
    TablesMeta,
} from '../types'
import { numericColTypes } from '../utils/colTypes'
import { db } from '.'
import logger from '../logger'

export const tablesMeta: { [key: string]: TablesMeta } = {}

export async function pullTableMeta(tablePath: string) {
    const [schema, table] = tablePath.split('.')

    // Get all table constraints.
    const constraints = await getTableConstraints(tablePath, [
        ConstraintType.PrimaryKey,
        ConstraintType.ForeignKey,
        ConstraintType.Unique,
        ConstraintType.UniqueIndex,
    ])
    if (!constraints?.length) {
        logger.error(`Couldn't find any table constraints for table: ${tablePath}`)
        return
    }

    // Bucket constraints by type, adding unique + unique-index to the same bucket.
    let primaryKeyConstraint
    const foreignKeyConstraints = []
    const uniqueConstraints = []
    for (const constraint of constraints) {
        switch (constraint.type) {
            case ConstraintType.PrimaryKey:
                primaryKeyConstraint = constraint
                break
            case ConstraintType.ForeignKey:
                foreignKeyConstraints.push(constraint)
                break
            case ConstraintType.Unique:
            case ConstraintType.UniqueIndex:
                uniqueConstraints.push(constraint)
                break
        }
    }

    // Get all foreign keys.
    const foreignTablePaths = foreignKeyConstraints.map((c) =>
        [c.parsed.foreignSchema, c.parsed.foreignTable].join('.')
    )
    const foreignKeys = await Promise.all(
        foreignTablePaths.map((foreignTablePath) =>
            getRelationshipBetweenTables(tablePath, foreignTablePath, foreignKeyConstraints)
        )
    )

    const [primaryKey, uniqueColGroups, colTypes] = await Promise.all([
        getPrimaryKeys(tablePath, true, primaryKeyConstraint),
        getUniqueColGroups(tablePath, uniqueConstraints),
        getColTypes(tablePath, [], false),
    ])

    // Map col types by name.
    const colTypesMap = {}
    for (const colType of colTypes) {
        colTypesMap[colType.name] = colType.type
    }

    // Register table with the global metadata holder.
    tablesMeta[tablePath] = {
        schema,
        table,
        primaryKey: primaryKey as DBColumn[],
        foreignKeys,
        uniqueColGroups,
        colTypes: colTypesMap,
    }
}

export function getRel(tablePath: string, foreignTablePath: string): ForeignKeyConstraint | null {
    const [foreignSchema, foreignTable] = foreignTablePath.split('.')
    const meta = tablesMeta[tablePath]
    return (
        meta?.foreignKeys.find(
            (fk) => fk.foreignSchema === foreignSchema && fk.foreignTable === foreignTable
        ) || null
    )
}

export function getTableColNames(tablePath: string): string[] {
    const meta = tablesMeta[tablePath]
    return Object.keys(meta?.colTypes || {})
}

export async function getTableConstraints(
    tablePath: string,
    constraintTypes?: ConstraintType | ConstraintType[]
): Promise<Constraint[]> {
    const [schema, table] = tablePath.split('.')
    constraintTypes = Array.isArray(constraintTypes)
        ? constraintTypes
        : constraintTypes
        ? [constraintTypes]
        : Object.values(ConstraintType)
    const requiredConstraintTypes = new Set<string>(constraintTypes)

    let rawConstraints = []
    if (
        requiredConstraintTypes.has(ConstraintType.ForeignKey) ||
        requiredConstraintTypes.has(ConstraintType.PrimaryKey) ||
        requiredConstraintTypes.has(ConstraintType.Unique)
    ) {
        const query = db.raw(
            `SELECT
                pg_get_constraintdef(c.oid) AS constraint,
                contype,
                conname
            FROM pg_constraint c 
            JOIN pg_namespace n 
                ON n.oid = c.connamespace 
            WHERE contype IN ('p', 'f', 'u')
            AND n.nspname = ?
            AND conrelid::regclass::text IN (?, ?, ?, ?, ?)`,
            [
                schema,
                table,
                `"${table}"`,
                tablePath,
                `"${schema}"."${table}"`,
                `${schema}."${table}"`,
            ]
        )

        const { rows } = await query
        rawConstraints = rows
    }

    if (requiredConstraintTypes.has(ConstraintType.UniqueIndex)) {
        const existingConstraintNames = new Set<string>(rawConstraints.map((c) => c.conname))

        const { rows } = await db.raw(
            `SELECT
                indexname as conname,
                indexdef as constraint
            FROM pg_indexes 
            WHERE schemaname = ? 
            AND (tablename = ? OR tablename = ?)`,
            [schema, table, `"${table}"`]
        )

        const otherUniqueIndexes = rows
            .filter(
                (row) =>
                    row.constraint.toLowerCase().includes('unique index') &&
                    !existingConstraintNames.has(row.conname)
            )
            .map((row) => ({ ...row, contype: ConstraintType.UniqueIndex }))

        rawConstraints = [...rawConstraints, ...otherUniqueIndexes]
    }

    return rawConstraints
        .map((row) => {
            let constraint = {
                type: row.contype,
                raw: row.constraint,
                parsed: {},
            }

            if (!requiredConstraintTypes.has(constraint.type)) {
                return null
            }

            switch (constraint.type) {
                case ConstraintType.ForeignKey:
                    constraint.parsed = parseForeignKeyConstraint(constraint.raw, schema) || {}
                    break
                case ConstraintType.PrimaryKey:
                case ConstraintType.Unique:
                case ConstraintType.UniqueIndex:
                    constraint.parsed = parseColNamesFromConstraint(constraint.raw) || {}
                    break
                default:
                    break
            }

            return constraint
        })
        .filter((c) => !!c)
}

export async function getPrimaryKeys(
    tablePath: string,
    includeTypes: boolean = false,
    constraint?: Constraint
): Promise<string[] | DBColumn[]> {
    const primaryKeyConstraint =
        constraint || ((await getTableConstraints(tablePath, [ConstraintType.PrimaryKey])) || [])[0]

    const colNames = primaryKeyConstraint?.parsed?.colNames || []
    if (!colNames.length) return []

    if (includeTypes) {
        return await getColTypes(tablePath, colNames)
    }

    return colNames
}

export async function getRelationshipBetweenTables(
    tablePath: string,
    foreignTablePath: string,
    constraints?: Constraint[]
): Promise<ForeignKeyConstraint | null> {
    const [schema, table] = tablePath.split('.')
    const [foreignSchema, foreignTable] = foreignTablePath.split('.')
    const foreignConstraints =
        constraints || (await getTableConstraints(tablePath, ConstraintType.ForeignKey))
    if (!foreignConstraints.length) return null

    const rels = []
    for (const constraint of foreignConstraints) {
        const { parsed } = constraint
        if (!parsed) continue
        if (
            (parsed.foreignSchema &&
                parsed.foreignSchema === foreignSchema &&
                parsed.foreignTable === foreignTable) ||
            (!parsed.foreignSchema &&
                foreignSchema === schema &&
                parsed.foreignTable === foreignTable)
        ) {
            rels.push(parsed)
        }
    }
    if (!rels.length) return null

    let rel = rels[0]
    if (rels.length > 1) {
        const relsWithIdRefKey = rels.filter((rel) => rel.referenceKey.includes('id'))
        rel = relsWithIdRefKey[0] || rel
    }

    return {
        schema,
        table,
        foreignSchema,
        foreignTable,
        foreignKey: rel.foreignKey,
        referenceKey: rel.referenceKey,
    }
}

export async function getUniqueColGroups(
    tablePath: string,
    constraints?: Constraint[]
): Promise<string[][]> {
    const uniqueConstraints =
        constraints ||
        (await getTableConstraints(tablePath, [ConstraintType.Unique, ConstraintType.UniqueIndex]))
    if (!uniqueConstraints.length) return []

    const seen = new Set<string>()
    const colNameGroups = []

    for (const constraint of uniqueConstraints) {
        const { colNames } = constraint.parsed
        if (!colNames || !colNames.length) continue
        const key = colNames.join(':')
        if (seen.has(key)) continue
        seen.add(key)
        colNameGroups.push(colNames)
    }

    return colNameGroups
}

export async function getColTypes(
    tablePath: string,
    colNames: string[],
    groupNumericTypes: boolean = true
): Promise<DBColumn[]> {
    const [schema, table] = tablePath.split('.')
    let query = `SELECT
            column_name as name,
            data_type as type
        FROM
            information_schema.columns
        WHERE
            table_schema = ? AND
            (table_name = ? OR table_name = ?)`
    let bindings = [schema, table, `"${table}"`]
    if (colNames?.length) {
        query += ` AND column_name in (${colNames.map((c) => '?').join(', ')})`
        bindings.push(...colNames)
    }

    const { rows } = await db.raw(query, bindings)

    if (!groupNumericTypes) {
        return rows || []
    }

    return rows.map((col) => {
        const type = numericColTypes.has(col.type) ? 'number' : col.type
        return { name: col.name, type }
    })
}

export function parseForeignKeyConstraint(
    raw: string,
    fallbackSchema: string
): StringKeyMap | null {
    const matches = raw.match(
        /FOREIGN KEY \(([-a-zA-Z0-9_", ]+)\) REFERENCES ([-a-zA-Z0-9_."]+)\(([-a-zA-Z0-9_", ]+)\)/i
    )
    if (!matches || matches.length !== 4) return null

    let [_, foreignKey, foreignTableOrPath, referenceKey] = matches

    let foreignSchema = null
    let foreignTable = foreignTableOrPath
    if (foreignTableOrPath.includes('.')) {
        const foreignTablePath = foreignTableOrPath.split('.')
        if (foreignTablePath.length !== 2) return null
        ;[foreignSchema, foreignTable] = foreignTablePath
    }

    const foreignKeyCols = foreignKey.split(',').map((v) => v.trim().replace(/"/gi, ''))
    const referenceKeyCols = referenceKey.split(',').map((v) => v.trim().replace(/"/gi, ''))
    return {
        foreignSchema: foreignSchema || fallbackSchema,
        foreignTable: foreignTable.replace(/"/gi, ''),
        foreignKey: foreignKeyCols,
        referenceKey: referenceKeyCols,
    }
}

export function parseColNamesFromConstraint(raw: string): StringKeyMap | null {
    const matches = raw.match(/\(([-a-zA-Z0-9_", ]+)\)/i)
    if (!matches || matches.length !== 2) return null
    const colNames = matches[1]
        .split(',')
        .map((col) => col.trim().replace(/"/gi, ''))
        .sort()
    return { colNames }
}

export function isColTypeArray(colPath: string): boolean {
    const [schema, table, colName] = colPath.split('.')
    const tablePath = [schema, table].join('.')

    const tableColTypes = (tablesMeta[tablePath] || {}).colTypes
    if (!tableColTypes) return false

    const colType = tableColTypes[colName]
    if (!colType) return false

    return colType.includes('[]') || colType.toLowerCase().includes('array')
}
