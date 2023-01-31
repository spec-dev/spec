export const numericColTypes = new Set([
    'smallint',
    'integer',
    'bigint',
    'decimal',
    'numeric',
    'real',
    'double precision',
    'smallserial',
    'serial',
    'bigserial',
])

export const isJSONColType = (colType: string): boolean => !!colType.match(/json/gi)

export const isTimestampColType = (colType: string): boolean => !!colType.match(/timestamp/gi)

export const isDateColType = (colType: string): boolean => colType === 'date'