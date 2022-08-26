import { ConstraintType, Constraint, ForeignKeyConstraint, StringKeyMap, DBColumn, TablesMeta } from '../types'
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
    const foreignTablePaths = foreignKeyConstraints.map(c => [
        c.parsed.foreignSchema,
        c.parsed.foreignTable
    ].join('.'))
    const foreignKeys = await Promise.all(foreignTablePaths.map(foreignTablePath => 
        getRelationshipBetweenTables(tablePath, foreignTablePath, foreignKeyConstraints)
    ))

    // Get primary key and unique column groups.
    const [
        primaryKey,
        uniqueColGroups,
    ] = await Promise.all([
        getPrimaryKeys(tablePath, true, primaryKeyConstraint),
        getUniqueColGroups(tablePath, uniqueConstraints),
    ])

    // Register table metadata for path.
    tablesMeta[tablePath] = {
        schema,
        table,
        primaryKey: primaryKey as DBColumn[],
        foreignKeys,
        uniqueColGroups,
    }
}

export function getRel(tablePath: string, foreignTablePath: string): ForeignKeyConstraint | null {
    const [foreignSchema, foreignTable] = foreignTablePath.split('.')
    const meta = tablesMeta[tablePath]
    return meta?.foreignKeys.find(fk => fk.foreignSchema === foreignSchema && fk.foreignTable === foreignTable) || null
}

export async function getTableConstraints(
    tablePath: string, 
    constraintTypes?: ConstraintType | ConstraintType[],
): Promise<Constraint[]> {
    const [schema, table] = tablePath.split('.')
    constraintTypes = Array.isArray(constraintTypes) 
        ? constraintTypes 
        : (constraintTypes ? [constraintTypes] : Object.values(ConstraintType))
    const requiredConstraintTypes = new Set<string>(constraintTypes)

    let rawConstraints = []
    if (requiredConstraintTypes.has(ConstraintType.ForeignKey) || 
        requiredConstraintTypes.has(ConstraintType.PrimaryKey) || 
        requiredConstraintTypes.has(ConstraintType.Unique)
    ) {
        const { rows } = await db.raw(
            `SELECT
                pg_get_constraintdef(c.oid) AS constraint,
                contype,
                conname
            FROM pg_constraint c 
            JOIN pg_namespace n 
                ON n.oid = c.connamespace 
            WHERE contype IN ('p', 'f', 'u')
            AND n.nspname = ?
            AND conrelid::regclass::text = ?`,
            [schema, table]
        )
        rawConstraints = rows
    }

    if (requiredConstraintTypes.has(ConstraintType.UniqueIndex)) {
        const existingConstraintNames = new Set<string>(rawConstraints.map(c => c.conname))

        const { rows } = await db.raw(
            `SELECT
                indexname as conname,
                indexdef as constraint
            FROM pg_indexes 
            WHERE schemaname = ? 
            AND tablename = ?`,
            [schema, table]
        )

        const otherUniqueIndexes = rows.filter(row => (
            row.constraint.toLowerCase().includes('unique index')) && !existingConstraintNames.has(row.conname)
        ).map(row => ({ ...row, contype: ConstraintType.UniqueIndex }))

        rawConstraints = [...rawConstraints, ...otherUniqueIndexes]
    }

    return rawConstraints.map(row => {
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
    }).filter(c => !!c)
}

export async function getPrimaryKeys(
    tablePath: string, 
    includeTypes: boolean = false, 
    constraint?: Constraint,
): Promise<string[] | DBColumn[]> {
    const primaryKeyConstraint = constraint || ((await getTableConstraints(tablePath, [
        ConstraintType.PrimaryKey,
    ])) || [])[0]

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
    constraints?: Constraint[],
): Promise<ForeignKeyConstraint | null> {
    const [schema, table] = tablePath.split('.')
    const [foreignSchema, foreignTable] = foreignTablePath.split('.')
    const foreignConstraints = constraints || (await getTableConstraints(tablePath, ConstraintType.ForeignKey))
    if (!foreignConstraints.length) return null

    const rels = []
    for (const constraint of foreignConstraints) {
        const { parsed } = constraint
        if (!parsed) continue
        if (
            (parsed.foreignSchema && parsed.foreignSchema === foreignSchema && parsed.foreignTable === foreignTable) ||
            (!parsed.foreignSchema && foreignSchema === schema && parsed.foreignTable === foreignTable)
        ) {
            rels.push(parsed)
        }
    }
    if (!rels.length) return null

    let rel = rels[0]
    if (rels.length > 1) {
        const relsWithIdRefKey = rels.filter(rel => rel.referenceKey === 'id')
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

export async function getUniqueColGroups(tablePath: string, constraints?: Constraint[]): Promise<string[][]> {
    const uniqueConstraints = constraints || (await getTableConstraints(tablePath, [
        ConstraintType.Unique,
        ConstraintType.UniqueIndex,
    ]))
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
    groupNumerics: boolean = true,
): Promise<DBColumn[]> {
    const [schema, table] = tablePath.split('.')
    const { rows } = await db.raw(
        `SELECT
            column_name as name,
            data_type as type
        FROM
            information_schema.columns
        WHERE
            table_schema = ? AND
            table_name = ? AND
            column_name in (${colNames.map(c => '?').join(', ')});`,
        [schema, table, ...colNames],
    )
    if (!groupNumerics) return rows || []

    return rows.map(col => {
        const type = numericColTypes.has(col.type) ? 'number' : col.type
        return { name: col.name, type }
    })
}

export function parseForeignKeyConstraint(raw: string, fallbackSchema: string): StringKeyMap | null {
    const matches = raw.match(/FOREIGN KEY \(([a-zA-Z0-9_-]+)\) REFERENCES ([a-zA-Z0-9_.-]+)\(([a-zA-Z0-9_-]+)\)/i)
    if (!matches || matches.length !== 4) return null

    const [_, foreignKey, foreignTableOrPath, referenceKey] = matches

    let foreignSchema = null
    let foreignTable = foreignTableOrPath
    if (foreignTableOrPath.includes('.')) {
        const foreignTablePath = foreignTableOrPath.split('.')
        if (foreignTablePath.length !== 2) return null
        ;([foreignSchema, foreignTable] = foreignTablePath)
    }

    return {
        foreignSchema: foreignSchema || fallbackSchema,
        foreignTable,
        foreignKey,
        referenceKey,
    }
}

export function parseColNamesFromConstraint(raw: string): StringKeyMap | null {
    const matches = raw.match(/\(([a-zA-Z0-9-_, ]+)\)/i)
    if (!matches || matches.length !== 2) return null
    const colNames = matches[1].split(',').map(col => col.trim()).sort()
    return { colNames }
}