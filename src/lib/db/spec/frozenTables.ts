import { schema } from '..'
import { FROZEN_TABLES, SPEC_SCHEMA_NAME } from './names'
import logger from '../../logger'
import chalk from 'chalk'
import { camelizeKeys } from 'humps'
import { FrozenTable } from '../../types'

const frozenTables = (tx?) => schema(SPEC_SCHEMA_NAME, tx).from(FROZEN_TABLES)

const CONFLICT_COLUMNS = ['table_path', 'chain_id']

export async function getFrozenTablesForChainId(chainId: string): Promise<FrozenTable[]> {
    try {
        return camelizeKeys(await frozenTables().where('chain_id', chainId))
    } catch (err) {
        logger.error(`Error getting frozen_tables for chain id ${chainId}: ${err}`)
        return []
    }
}

export async function freezeTablesForChainId(tablePaths: string | string[], chainId: string) {
    tablePaths = Array.isArray(tablePaths) ? tablePaths : [tablePaths]
    logger.error(chalk.red(`Freezing table(s) ${tablePaths.join(', ')} for chain id ${chainId}...`))
    try {
        await frozenTables()
            .insert(tablePaths.map((tablePath) => ({ table_path: tablePath, chain_id: chainId })))
            .onConflict(CONFLICT_COLUMNS)
            .ignore()
    } catch (err) {
        logger.error(`Error saving frozen_table(s) for chain id ${chainId}): ${err}`)
    }
}
