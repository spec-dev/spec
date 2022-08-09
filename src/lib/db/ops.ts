import { db } from './index'
import { ConstraintType, Constraint, ForeignKeyConstraint, StringMap } from '../types'

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

export async function getTableConstraints(tablePath: string, constraintType: ConstraintType): Promise<Constraint[]> {
    const [schema, table] = tablePath.split('.')
    const result = await db.raw(
        `SELECT 
            pg_get_constraintdef(c.oid) AS constraint
        FROM pg_constraint c 
        JOIN pg_namespace n 
            ON n.oid = c.connamespace 
        WHERE contype IN (?) 
        AND n.nspname = ?
        AND conrelid::regclass::text = ?`,
        [constraintType, schema, table]
    )
    
    return (result?.rows || []).map(row => {
        let constraint = { 
            type: constraintType, 
            raw: row.constraint,
            parsed: {},
        }
        
        switch (constraint.type) {
            case ConstraintType.ForeignKey:
                constraint.parsed = parseForeignKeyConstraint(constraint.raw) || {}
                break
            case ConstraintType.Unique:
                constraint.parsed = parseUniqueConstraint(constraint.raw) || {}
                break
            default:
                break
        }

        return constraint
    })
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

function parseForeignKeyConstraint(raw: string): StringMap | null {
    const matches = raw.match(/FOREIGN KEY \(([a-zA-Z0-9_]+)\) REFERENCES ([a-zA-Z0-9_.]+)\(([a-zA-Z0-9_]+)\)/i)
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

function parseUniqueConstraint(raw: string): StringMap | null {
    return {}
}