import { LiveObject, LiveObjectLink, StringKeyMap, Op, OpType, EnrichedLink } from '../types'
import { SpecEvent } from '@spec.dev/event-client'
import ApplyDiffsService from './ApplyDiffsService'
import logger from '../logger'
import { db } from '../db'
import RunOpService from './RunOpService'
import config from '../config'
import chalk from 'chalk'

class ApplyEventService {
    
    event: SpecEvent

    liveObject: LiveObject

    liveObjectDiffs: StringKeyMap[] = []

    linksToApplyDiffsTo: EnrichedLink[] = []

    ops: Op[] = []

    get links(): LiveObjectLink[] {
        return this.liveObject.links || []
    }

    get wasSkipped(): boolean {
        return (this.event.origin as StringKeyMap).skipped === true
    }

    get isReplay(): boolean {
        return (this.event.origin as StringKeyMap).replay === true
    }

    constructor(event: SpecEvent, liveObject: LiveObject) {
        this.event = event
        this.liveObject = liveObject
        if (!this.links.length) throw 'Live object has no links...'
    }

    async perform() {
        const origin = this.event.origin
        const chainId = origin?.chainId
        const blockNumber = origin?.blockNumber

        logger.info(
            `[${chainId}:${blockNumber}] Processing ${this.event.name} (${this.event.nonce})...`
        )

        // Format event data as an array of diffs.
        const data = this.event.data
        this.liveObjectDiffs = Array.isArray(data) ? data : [data]
        if (!this.liveObjectDiffs.length) return

        // Get all links these diffs apply to (i.e. the links with all of their 
        // implemented property keys included in the diff structure).
        this.linksToApplyDiffsTo = this._getLinksToApplyDiffTo()
        if (!this.linksToApplyDiffsTo.length) {
            logger.info(`Live object diff didn't satisfy any configured links`)
            return
        }

        await this.getOps()
        await this.runOps()
    }

    _getLinksToApplyDiffTo(): EnrichedLink[] {
        const linksToApplyDiffsTo = []
        for (const link of this.links) {
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
            const service = new ApplyDiffsService(this.liveObjectDiffs, enrichedLink, this.liveObject)
            promises.push(service.getOps())
        }
        this.ops = (await Promise.all(promises)).flat()
        return this.ops
    }

    async runOps() {
        if (!this.ops.length) return

        for (const op of this.ops) {
            if (op.type === OpType.Insert) {
                const numUpserts = op.data.length
                logger.info(
                    chalk.green(`Upserting ${numUpserts} records in ${op.schema}.${op.table}...`)
                )
            }
        }

        const allowUpdatesBackwardsInTime = this.wasSkipped || this.isReplay
        await db.transaction(async (tx) => {
            await Promise.all(this.ops.map((op) => new RunOpService(op, tx, allowUpdatesBackwardsInTime).perform()))
        })
    }
}

export default ApplyEventService