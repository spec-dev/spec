import { db } from './index'

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