import { LiveObject, LiveObjectLink, StringKeyMap, Op } from '../types'
import { SpecEvent } from '@spec.dev/event-client'
import ApplyDiffsService from './ApplyDiffsService'
import logger from '../logger'
import { db } from '../db'
import RunOpService from './RunOpService'

class ApplyEventService {

    event: SpecEvent<StringKeyMap | StringKeyMap[]>

    liveObject: LiveObject

    linksToApplyDiffTo: LiveObjectLink[] = []

    ops: Op[] = []

    get liveObjectDiffs(): StringKeyMap[] {
        const data = this.event.data
        return Array.isArray(data) ? data : [data]
    }

    get links(): LiveObjectLink[] {
        return this.liveObject.links || []
    }

    constructor(event: SpecEvent<StringKeyMap>, liveObject: LiveObject) {
        this.event = event
        this.liveObject = liveObject
        if (!this.links.length) throw 'Live object has no links...'
    }

    async perform() {
        logger.info('Applying event...', this.event)
        await this.getOps()
        await this.runOps()
    }

    async getOps(): Promise<Op[]> {
        // Get all links this diff applies to (i.e. the links 
        // who have all of their property keys included in this diff).
        this.linksToApplyDiffTo = this._getLinksToApplyDiffTo()
        if (!this.linksToApplyDiffTo.length) {
            logger.info('Live object diff didn\'t satisfy any configured links')
            return this.ops
        }

        // Get ops to apply the diffs for each link.
        const diffs = this.liveObjectDiffs
        let promises = []
        for (let link of this.linksToApplyDiffTo) {
            promises.push(new ApplyDiffsService(diffs, link, this.liveObject).getOps())
        }

        this.ops = (await Promise.all(promises)).flat()
        return this.ops
    }

    async runOps() {
        if (!this.ops.length) return
        await db.transaction(async tx => {
            await Promise.all(this.ops.map(op => new RunOpService(op, tx).perform()))
        })
    }

    _getLinksToApplyDiffTo(): LiveObjectLink[] {
        const linksToApplyDiffTo = []
        const liveObjectDiffs = this.liveObjectDiffs
        for (const link of this.links) {
            let allUniquePropertiesIncludedInDiff = true
            for (const property in link.properties) {
                for (const diff of liveObjectDiffs) {
                    if (!diff.hasOwnProperty(property)) {
                        allUniquePropertiesIncludedInDiff = false
                        break
                    }
                }
                if (!allUniquePropertiesIncludedInDiff) break
            }
            if (allUniquePropertiesIncludedInDiff) {
                linksToApplyDiffTo.push(link)
            }
        }
        return linksToApplyDiffTo
    }    
}

export default ApplyEventService