import { schema } from '..'
import { OPS_TABLE_NAME, SPEC_SCHEMA_NAME } from './names'
import { OpRecord } from '../../types'
import { camelizeKeys } from 'humps'

const ops = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(OPS_TABLE_NAME)

export async function getDistinctRecordsOperatedOnAtOrAboveBlockNumber(
    blockNumber: number,
    chainId: string,
): Promise<OpRecord[]> {
    return camelizeKeys(await ops()
        .distinctOn(['table_path', 'pk_values'])
        .where('block_number', '>=', blockNumber)
        .andWhere('chain_id', chainId)
        .orderBy([
            { column: 'table_path', order: 'asc' },
            { column: 'pk_values', order: 'asc' },
            { column: 'block_number', order: 'asc' },
            { column: 'ts', order: 'asc' },
        ])
    )
}

export async function deleteTableOpsAtOrAboveNumber(
    tablePath: string,
    blockNumber: number,
    chainId: string,
    tx?: any
) {
    await ops(tx)
        .where('table_path', tablePath)
        .andWhere('block_number', '>=', blockNumber)
        .andWhere('chain_id', chainId)
        .del()
}
