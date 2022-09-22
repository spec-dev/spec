import { StringKeyMap } from '../types'
import { db } from '../db'

export const updatedAtColNames = [
    'updatedAt',
    'updated_at',
]

export function getUpdatedAtColName(data: StringKeyMap): string | null {
    return Object.keys(data).find(key => updatedAtColNames.includes(key)) || null
}

export const now = () => db.raw('CURRENT_TIMESTAMP')