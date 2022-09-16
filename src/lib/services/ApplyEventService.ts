import { LiveObject, LiveObjectLink, StringKeyMap, Op } from '../types'
import { SpecEvent } from '@spec.dev/event-client'
import ApplyDiffsService from './ApplyDiffsService'
import logger from '../logger'
import { db } from '../db'
import RunOpService from './RunOpService'

class ApplyEventService {
    event: SpecEvent<StringKeyMap | StringKeyMap[]>

    liveObject: LiveObject

    liveObjectDiffs: StringKeyMap[] = []

    linksToApplyDiffsTo: LiveObjectLink[] = []

    ops: Op[] = []

    get links(): LiveObjectLink[] {
        return this.liveObject.links || []
    }

    constructor(event: SpecEvent<StringKeyMap>, liveObject: LiveObject) {
        this.event = event
        this.liveObject = liveObject
        if (!this.links.length) throw 'Live object has no links...'
    }

    async perform() {
        logger.info(`[${this.event.name}] Processing event...`)
        this._filterLiveObjectDiffs()
        await this.getOps()
        await this.runOps()
    }

    async getOps(): Promise<Op[]> {
        // Get all links these diffs apply to (i.e. the links who have
        // all of their property keys included in the diff structure).
        this.linksToApplyDiffsTo = this._getLinksToApplyDiffTo()
        if (!this.linksToApplyDiffsTo.length) {
            logger.info("Live object diff didn't satisfy any configured links")
            return this.ops
        }

        // Get ops to apply the diffs for each link.
        let promises = []
        for (let link of this.linksToApplyDiffsTo) {
            promises.push(
                new ApplyDiffsService(this.liveObjectDiffs, link, this.liveObject).getOps()
            )
        }

        this.ops = (await Promise.all(promises)).flat()

        return this.ops
    }

    async runOps() {
        if (!this.ops.length) return
        logger.info(`Event generated ${this.ops.length} ops.`)
        await db.transaction(async (tx) => {
            await Promise.all(this.ops.map((op) => new RunOpService(op, tx).perform()))
        })
    }

    _filterLiveObjectDiffs() {
        const data = this.event.data
        const allDiffs = Array.isArray(data) ? data : [data]

        const liveObjectFilters = this.liveObject.filterBy || {}
        if (!Object.keys(liveObjectFilters).length) {
            this.liveObjectDiffs = allDiffs
            return
        }

        const diffsToProcess = []
        for (const diff of allDiffs) {
            let processDiff = true

            for (const property in liveObjectFilters) {
                if (!diff.hasOwnProperty(property)) {
                    processDiff = false
                    break
                }

                const acceptedValue = liveObjectFilters[property]
                const acceptedValueIsArray = Array.isArray(acceptedValue)
                const givenValue = diff[property]

                if (
                    (acceptedValueIsArray && !acceptedValue.includes(givenValue)) ||
                    (!acceptedValueIsArray && givenValue !== acceptedValue)
                ) {
                    processDiff = false
                    break
                }
            }
            processDiff && diffsToProcess.push(diff)
        }

        this.liveObjectDiffs = diffsToProcess
    }

    _getLinksToApplyDiffTo(): LiveObjectLink[] {
        const linksToApplyDiffsTo = []

        for (const link of this.links) {
            let allLinkPropertiesIncludedInDiff = true

            for (const property in link.inputs) {
                for (const diff of this.liveObjectDiffs) {
                    if (!diff.hasOwnProperty(property)) {
                        allLinkPropertiesIncludedInDiff = false
                        break
                    }
                }
                if (!allLinkPropertiesIncludedInDiff) break
            }
            if (allLinkPropertiesIncludedInDiff) {
                linksToApplyDiffsTo.push(link)
            }
        }

        return linksToApplyDiffsTo
    }
}

export default ApplyEventService
