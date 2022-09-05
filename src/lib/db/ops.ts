import { db } from './index'

export async function doesSchemaExist(schema: string): Promise<boolean> {
    const result = await db.from('information_schema.schemata').where({ schema_name: schema }).count()
    const count = result ? Number((result[0] || {}).count || 0) : 0
    return count > 0
}

export async function doesTableExist(table: string, schema: string): Promise<boolean> {
    const result = await db.from('pg_tables').where({ schemaname: schema, tablename: table }).count()
    const count = result ? Number((result[0] || {}).count || 0) : 0
    return count > 0
}

export async function areColumnsEmpty(tablePath: string, colNames: string[]): Promise<boolean> {
    const whereConditions = {}
    for (let colName of colNames) {
        whereConditions[colName] = null
    }
    const result = await db.from(tablePath).whereNot(whereConditions).count()
    const count = result ? Number((result[0] || {}).count || 0) : 0
    return count === 0
}

export async function isTableEmpty(tablePath: string): Promise<boolean> {
    return (await tableCount(tablePath)) === 0
}

export async function tableCount(tablePath: string): Promise<number> {
    const result = await db.from(tablePath).count()
    return result ? Number((result[0] || {}).count || 0) : 0
}