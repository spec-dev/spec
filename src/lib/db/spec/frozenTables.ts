import { schema } from '..'
import { FROZEN_TABLES, SPEC_SCHEMA_NAME } from './names'
import logger from '../../logger'
import chalk from 'chalk'

const frozenTables = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(FROZEN_TABLES)

const CONFLICT_COLUMNS = [
    'table_path',
    'chain_id',
]

export async function freezeTableForChainId(tablePath: string, chainId: string) {
    logger.error(chalk.red(`Freezing table "${tablePath}" for chain id ${chainId}...`))
    try {
        await frozenTables()
            .insert({ table_path: tablePath, chain_id: chainId })
            .onConflict(CONFLICT_COLUMNS)
            .ignore()
    } catch (err) {
        logger.error(
            `Error saving frozen_table (tablePath=${tablePath}, chainId=${chainId}): ${err}`
        )
    }
}