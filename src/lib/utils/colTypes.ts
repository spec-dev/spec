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

export const isTimestampColType = (colType: string): boolean => !!colType.match(/timestamp/gi)