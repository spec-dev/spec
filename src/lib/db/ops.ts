import { db } from './index'

export async function doesSchemaExist(schema: string): Promise<boolean> {
    const result = await db.raw(
        'SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?', 
        [schema],
    )
    return result.rowCount > 0
}

export async function doesTableExist(table: string, schema: string): Promise<boolean> {
    const result = await db.raw(
        'SELECT COUNT(*) FROM pg_tables WHERE schemaname = ? AND tablename = ?', 
        [schema, table],
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