import { db } from './index'
import { ConstraintType, Constraint, ForeignKeyConstraint, StringKeyMap, DBColumn } from '../types'
import { numericColTypes } from '../utils/colTypes'

export async function doesSchemaExist(name: string): Promise<boolean> {
    const result = await db.raw(
        'SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?', 
        [name],
    )
    return result.rowCount > 0
}

export async function doesTableExist(name: string, schema: string = 'public'): Promise<boolean> {
    const result = await db.raw(
        'SELECT COUNT(*) FROM pg_tables WHERE schemaname = ? AND tablename = ?', 
        [schema, name],
    )
    return result.rowCount > 0
}

export async function areColumnsEmpty(tablePath: string, colNames: string[]): Promise<boolean> {
    const whereConditions = {}
    for (let colName of colNames) {
        whereConditions[colName] = null
    }

    const result = await db.from(tablePath).whereNot(whereConditions).count()
    const count = Number(result[0].count)
    return count === 0
}

export async function isTableEmpty(tablePath: string): Promise<boolean> {
    return (await tableCount(tablePath)) === 0
}

export async function tableCount(tablePath: string): Promise<number> {
    const result = await db.from(tablePath).count()
    return result ? Number((result[0] || {}).count || 0) : 0
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
                constraint.parsed = parseForeignKeyConstraint(constraint.raw) || {}
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

export async function getRelationshipBetweenTables(
    tablePath: string, 
    foreignTablePath: string,
): Promise<ForeignKeyConstraint | null> {
    const [schema, table] = tablePath.split('.')
    const [foreignSchema, foreignTable] = foreignTablePath.split('.')
    const foreignConstraints = await getTableConstraints(tablePath, ConstraintType.ForeignKey)
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

export async function getUniqueColGroups(tablePath: string): Promise<string[][]> {
    const uniqueConstraints = await getTableConstraints(tablePath, [
        ConstraintType.PrimaryKey,
        ConstraintType.Unique,
        ConstraintType.UniqueIndex,
    ])
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

export async function getPrimaryKeys(tablePath: string, includeTypes: boolean = false): Promise<string[] | DBColumn[]> {
    const primaryKeyConstraint = ((await getTableConstraints(tablePath, [
        ConstraintType.PrimaryKey,
    ])) || [])[0]

    const colNames = primaryKeyConstraint?.parsed?.colNames || []
    if (!colNames.length) return []

    if (includeTypes) {
        return await getColTypes(tablePath, colNames)
    }
    
    return colNames
}

function parseForeignKeyConstraint(raw: string): StringKeyMap | null {
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
        foreignSchema,
        foreignTable,
        foreignKey,
        referenceKey,
    }
}

function parseColNamesFromConstraint(raw: string): StringKeyMap | null {
    const matches = raw.match(/\(([a-zA-Z0-9-_, ]+)\)/i)
    if (!matches || matches.length !== 2) return null
    const colNames = matches[1].split(',').map(col => col.trim()).sort()
    return { colNames }
}