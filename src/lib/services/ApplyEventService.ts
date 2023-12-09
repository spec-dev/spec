import { LiveObject, LiveObjectLink, StringKeyMap, Op, OpType, EnrichedLink } from '../types'
import { SpecEvent } from '@spec.dev/event-client'
import ApplyDiffsService from './ApplyDiffsService'
import logger from '../logger'
import { fromNamespacedVersion } from '../utils/formatters'
import { db } from '../db'
import RunOpService from './RunOpService'
import { getFrozenTablesForChainId } from '../db/spec/frozenTables'
import config from '../config'
import { constants } from '../constants'
import chalk from 'chalk'
import { isPrimitiveNamespace } from '../utils/chains'

class ApplyEventService {
    event: SpecEvent

    liveObject: LiveObject

    liveObjectDiffs: StringKeyMap[] = []

    linksToApplyDiffsTo: EnrichedLink[] = []

    ops: Op[] = []

    allTablesFrozen: boolean = false

    get links(): LiveObjectLink[] {
        return this.liveObject.links || []
    }

    constructor(event: SpecEvent, liveObject: LiveObject) {
        this.event = event
        this.liveObject = liveObject
        if (!this.links.length) throw 'Live object has no links...'
    }

    async perform() {
        const origin = this.event.origin
        const chainId = origin?.chainId

        // Format event data as an array of diffs.
        const data = this.event.data
        this.liveObjectDiffs = Array.isArray(data) ? data : [data]
        if (!this.liveObjectDiffs.length) return

        // Get all links these diffs apply to (i.e. the links with all of their
        // implemented property keys included in the diff structure).
        this.linksToApplyDiffsTo = await this._getLinksToApplyDiffTo(chainId)
        if (!this.linksToApplyDiffsTo.length) {
            this.allTablesFrozen ||
                logger.warn(
                    `Live object diff didn't satisfy any configured links`,
                    JSON.stringify(this.event, null, 4)
                )
            return
        }

        await this.getOps()
        await this.runOps()
    }

    async _getLinksToApplyDiffTo(chainId: string): Promise<EnrichedLink[]> {
        const frozenTablePaths = new Set(
            (await getFrozenTablesForChainId(chainId)).map((r) => r.tablePath)
        )
        const linksToApplyDiffsTo = []
        for (const link of this.links) {
            if (frozenTablePaths.has(link.table)) {
                logger.info(
                    chalk.yellow(
                        `Not applying event to "${link.table}" -- table is frozen for chain ${chainId}.`
                    )
                )
                if (this.links.length === 1) {
                    this.allTablesFrozen = true
                }
                continue
            }
            const enrichedLink = config.getEnrichedLink(this.liveObject.id, link.table)
            if (!enrichedLink) continue

            let allLinkPropertiesIncludedInDiff = true
            for (const property in enrichedLink.linkOn) {
                for (const diff of this.liveObjectDiffs) {
                    if (!diff.hasOwnProperty(property) || diff[property] === null) {
                        allLinkPropertiesIncludedInDiff = false
                        break
                    }
                }
                if (!allLinkPropertiesIncludedInDiff) break
            }

            if (allLinkPropertiesIncludedInDiff) {
                linksToApplyDiffsTo.push(enrichedLink)
            }
        }
        return linksToApplyDiffsTo
    }

    async getOps(): Promise<Op[]> {
        let promises = []
        for (let enrichedLink of this.linksToApplyDiffsTo) {
            const service = new ApplyDiffsService(
                this.liveObjectDiffs,
                enrichedLink,
                this.liveObject
            )
            promises.push(service.getOps())
        }
        this.ops = (await Promise.all(promises)).flat()
        return this.ops
    }

    async runOps() {
        if (!this.ops.length) return

        const eventNsp = fromNamespacedVersion(this.event.name).nsp
        const logDelayedEvent =
            isPrimitiveNamespace(eventNsp) && !constants.LOG_PRIMITIVE_NSP_EVENTS
        if (logDelayedEvent) {
            const origin = this.event.origin
            const chainId = origin?.chainId
            const blockNumber = origin?.blockNumber
            logger.info(
                `[${chainId}:${blockNumber}] Processing ${this.event.name} (${this.event.nonce})...`
            )
        }

        for (const op of this.ops) {
            if (op.type === OpType.Insert) {
                const numUpserts = op.data.length
                logger.info(
                    chalk.green(`Upserting ${numUpserts} records in ${op.schema}.${op.table}...`)
                )
            }
        }

        await db.transaction(async (tx) => {
            await Promise.all(this.ops.map((op) => new RunOpService(op, tx).perform()))
        })
    }
}

export default ApplyEventService
