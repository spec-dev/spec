import logger from '../logger'
import { OpRecord, StringKeyMap, OpType } from '../types'
import { db } from '../db'
import chalk from 'chalk'
import { stringify, toChunks, sum } from '../utils/formatters'
import { sleep } from '../utils/time'
import { randomIntegerInRange } from '../utils/math'
import { constants } from '../constants'
import {
    getDistinctRecordsOperatedOnAtOrAboveBlockNumber,
    upsertOpTrackingEntries,
    deleteTableOpsAtOrAboveNumber,
    freezeTablesForChainId,
} from '../db/spec'

class RollbackService {
    blockNumber: number

    chainId: string

    recordSnapshotOps: { [key: string]: OpRecord[] } = {}

    get tablePaths(): string[] {
        return Object.keys(this.recordSnapshotOps)
    }

    constructor(blockNumber: number, chainId: string) {
        this.blockNumber = blockNumber
        this.chainId = chainId
    }

    async perform() {
        logger.debug(chalk.gray(`[${this.chainId}] Rolling back to block ${this.blockNumber}...`))

        // Get snapshots of records that need to be rolled back, indexed by table path.
        await this._getTargetRecordSnapshotOps()

        // Log rollback stats and return early if nothing to do.
        const needToRollback = this._logTablesAffectedByRollback()
        if (!needToRollback) return

        // Set the new "floor" for ops to be tracked at or above (block number).
        const opTrackingUpserts = this.tablePaths.map((tablePath) => ({
            tablePath,
            chainId: this.chainId,
            isEnabledAbove: this.blockNumber,
        }))
        if (!(await upsertOpTrackingEntries(opTrackingUpserts))) {
            throw `[${this.chainId}] Failed to set new op tracking floor during rollback to ${this.blockNumber}`
        }

        // Roll records back to their previous states prior to the target block number.
        await this._rollbackRecords()
        logger.info(chalk.green(`[${this.chainId}:${this.blockNumber}] Rollback complete.`))
    }

    async _getTargetRecordSnapshotOps() {
        let opRecords = []
        try {
            opRecords = await getDistinctRecordsOperatedOnAtOrAboveBlockNumber(
                this.blockNumber,
                this.chainId
            )
        } catch (err) {
            throw `[${this.chainId}] Failed getting ops >= ${this.blockNumber}: ${err}`
        }

        // Group by table path.
        for (const record of opRecords) {
            this.recordSnapshotOps[record.table_path] =
                this.recordSnapshotOps[record.table_path] || []
            this.recordSnapshotOps[record.table_path].push(record)
        }
    }

    async _rollbackRecords() {
        await Promise.all(
            this.tablePaths.map((tablePath) => this._rollbackRecordsForTable(tablePath))
        )
    }

    async _rollbackRecordsForTable(tablePath: string) {
        // Group op records by their rollback operation.
        const opRecords = this.recordSnapshotOps[tablePath]
        const [upsertGroups, deleteGroups] = this._getBatchRollbackOperations(tablePath, opRecords)
        const numUpserts = sum(upsertGroups.map((r) => r.length))
        const numDeletes = sum(deleteGroups.map((r) => r.length))
        logger.info(chalk.magenta(`- ${tablePath} | Upserts=${numUpserts} | Deletes=${numDeletes}`))

        let attempt = 1
        while (attempt <= constants.MAX_DEADLOCK_RETRIES) {
            try {
                await db.transaction(async (tx) => {
                    // Rollback records.
                    await Promise.all([
                        ...upsertGroups.map((records) =>
                            this._rollbackRecordsWithUpsertion(tablePath, records, tx)
                        ),
                        ...deleteGroups.map((records) =>
                            this._rollbackRecordsWithDeletion(tablePath, records, tx)
                        ),
                    ])

                    // Remove ops for this table at or above the target block number.
                    await deleteTableOpsAtOrAboveNumber(
                        tablePath,
                        this.blockNumber,
                        this.chainId,
                        tx
                    )
                })
                break
            } catch (err) {
                attempt++
                logger.error(
                    `[${this.chainId}] Error rolling back ${tablePath} >= ${this.blockNumber}`,
                    err
                )
                const message = err.message || err.toString() || ''

                // Wait and try again if deadlocked.
                if (message.toLowerCase().includes('deadlock')) {
                    logger.error(
                        `[${this.chainId}:${this.blockNumber} - Rolling back ${tablePath}] 
                        Got deadlock on attempt ${attempt}/${constants.MAX_DEADLOCK_RETRIES}.`
                    )
                    await sleep(randomIntegerInRange(50, 500))
                    continue
                }

                // If the rollback completely fails, "freeze" any further updates to it for this specific chain id.
                const error = `[${this.chainId}:${this.blockNumber}] Failed to rollback ops for ${tablePath}: ${err}`
                logger.error(error)
                await freezeTablesForChainId(tablePath, this.chainId)
                break
            }
        }
    }

    async _rollbackRecordsWithUpsertion(tablePath: string, opRecords: OpRecord[], tx: any) {
        const [schemaName, tableName] = tablePath.split('.')
        const rollbackGroups = this._groupUpsertOpsByRecordStructure(opRecords)
        const promises = []

        for (const key in rollbackGroups) {
            const upsertOps = rollbackGroups[key]
            const { conflictColNames, updateColNames, columns, upsert } = upsertOps[0]
            const placeholders = []
            const bindings = [schemaName, tableName, ...columns]

            for (const { values } of upsertOps) {
                const recordPlaceholders = []
                for (let j = 0; j < columns.length; j++) {
                    recordPlaceholders.push(`?`)
                    bindings.push(values[j])
                }
                placeholders.push(`(${recordPlaceholders.join(', ')})`)
            }

            let query = `insert into ??.?? (${columns
                .map(() => '??')
                .join(', ')}) values ${placeholders.join(', ')}`

            if (upsert) {
                bindings.push(...conflictColNames)
                const updateClause = []
                for (const colName of updateColNames) {
                    bindings.push(...[colName, colName])
                    updateClause.push(`?? = excluded.??`)
                }
                query += ` on conflict (${conflictColNames
                    .map(() => '??')
                    .join(', ')}) do update set ${updateClause.join(', ')}`
            }

            promises.push(tx.raw(query, bindings))
        }

        await Promise.all(promises)
    }

    async _rollbackRecordsWithDeletion(tablePath: string, opRecords: OpRecord[], tx: any) {
        const [schemaName, tableName] = tablePath.split('.')
        const orClauses = []
        const bindings = [schemaName, tableName]

        for (const opRecord of opRecords) {
            const pkNames = opRecord.pk_names.split(',').map((name) => name.trim())
            const pkValues = opRecord.pk_values.split(',').map((value) => value.trim())
            const andClauses = []
            for (let j = 0; j < pkNames.length; j++) {
                andClauses.push(`?? = ?`)
                bindings.push(...[pkNames[j], pkValues[j]])
            }
            orClauses.push(`(${andClauses.join(' and ')})`)
        }

        const conditions = orClauses.join(' or ')
        const query = `delete from ??.?? where ${conditions}`
        await tx.raw(query, bindings)
    }

    /**
     * Group these table ops by their table/record "structure" to handle
     * the case where the table schema has actually changed.
     */
    _groupUpsertOpsByRecordStructure(opRecords: OpRecord[]): StringKeyMap {
        const rollbackGroups = {}
        for (const opRecord of opRecords) {
            const conflictColNames = opRecord.pk_names.split(',').map((name) => name.trim())
            const conflictColNamesSet = new Set(conflictColNames)
            const conflictColValues = opRecord.pk_values.split(',').map((value) => value.trim())
            const updateColNames = []
            const updateColValues = []
            const sortedRecordKeys = Object.keys(opRecord.before).sort()

            for (const colName of sortedRecordKeys) {
                if (conflictColNamesSet.has(colName)) continue
                updateColNames.push(colName)

                // Re-stringify JSON column types.
                let colValue = opRecord.before[colName]
                if (colValue && typeof colValue === 'object') {
                    colValue = this._stringifyObjectTypeColValue(colName, colValue)
                }

                updateColValues.push(colValue)
            }

            const uniqueKey = ['c', ...conflictColNames, 'u', ...updateColNames].join(':')
            rollbackGroups[uniqueKey] = rollbackGroups[uniqueKey] || []
            rollbackGroups[uniqueKey].push({
                conflictColNames,
                updateColNames,
                columns: [...conflictColNames, ...updateColNames],
                values: [...conflictColValues, ...updateColValues],
                upsert: updateColNames.length > 0,
                opRecord,
            })
        }
        return rollbackGroups
    }

    _getBatchRollbackOperations(tablePath: string, opRecords: OpRecord[]) {
        const upserts = []
        const deletes = []
        for (const record of opRecords) {
            if (!record.before && !record.after) {
                logger.error(
                    `Got strange op with null values for both before 
                    and after (table=${tablePath}): ${stringify(record)}`
                )
                continue
            }
            const reverseOpType = this._getReverseOpType(this._determineOpTypeFromRecord(record))
            switch (reverseOpType) {
                case OpType.Insert:
                case OpType.Update:
                    upserts.push(record)
                    break
                case OpType.Delete:
                    deletes.push(record)
                    break
            }
        }

        // Split into reasonable-sized batches.
        const upsertGroups = toChunks(upserts, constants.ROLLBACK_BATCH_SIZE) as OpRecord[][]
        const deleteGroups = toChunks(deletes, constants.ROLLBACK_BATCH_SIZE) as OpRecord[][]

        return [upsertGroups, deleteGroups]
    }

    _determineOpTypeFromRecord(opRecord: OpRecord): OpType {
        const existedBefore = !!opRecord.before
        const existedAfter = !!opRecord.after

        if (!existedBefore) {
            return OpType.Insert
        }
        if (!existedAfter) {
            return OpType.Delete
        }
        return OpType.Update
    }

    _getReverseOpType(opType: OpType): OpType {
        if (opType === OpType.Insert) {
            return OpType.Delete
        }
        if (opType === OpType.Delete) {
            return OpType.Insert
        }
        return OpType.Update
    }

    _logTablesAffectedByRollback(): boolean {
        const stats = []
        let total = 0
        for (const tablePath in this.recordSnapshotOps) {
            const recordsAffected = this.recordSnapshotOps[tablePath]
            if (!recordsAffected.length) continue
            total += recordsAffected.length
            stats.push([tablePath, recordsAffected.length])
        }
        if (!stats.length) {
            logger.debug(chalk.gray(`[${this.chainId}] No records to roll back.`))
            return false
        }
        logger.info(
            chalk.magenta(
                `[${this.chainId}] Rolling back ${total} records across ${stats.length} table(s):`
            )
        )
        return true
    }

    _stringifyObjectTypeColValue(colName: string, value: any): any {
        const originalValue = value
        try {
            return JSON.stringify(value)
        } catch (err) {
            logger.error(`Error stringifying ${colName} during rollback: ${value} - ${err}`)
            return originalValue
        }
    }
}

export default RollbackService
