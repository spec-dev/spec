import { LiveObject, LiveObjectLink, StringKeyMap, Op } from '../types'
import { SpecEvent } from '@spec.dev/event-client'
import ApplyDiffService from './ApplyDiffService'
import logger from '../logger'
import { db } from '../db'
import RunOpService from './RunOpService'

class ApplyEventService {

    event: SpecEvent<StringKeyMap>

    liveObject: LiveObject

    linksToApplyDiffTo: LiveObjectLink[] = []

    ops: Op[] = []

    get liveObjectDiff(): StringKeyMap {
        return this.event.object
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
        // Get links this diff applies to (all links who have all of 
        // their "uniqueBy" property keys included in this diff).
        this.linksToApplyDiffTo = this._getLinksToApplyDiffTo()
        if (!this.linksToApplyDiffTo.length) {
            logger.info('Live object diff didn\'t satisfy any configured links')
            return this.ops
        }

        // Get ops to apply the diffs for each link.
        const diff = this.liveObjectDiff
        let promises = []
        for (let link of this.linksToApplyDiffTo) {
            promises.push(new ApplyDiffService(diff, link, this.liveObject.id).getOps())
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
        const liveObjectDiff = this.liveObjectDiff
        for (const link of this.links) {
            let allUniquePropertiesIncludedInDiff = true
            for (const property of link.uniqueBy) {
                if (!liveObjectDiff.hasOwnProperty(property)) {
                    allUniquePropertiesIncludedInDiff = false
                    break
                }
            }
            if (allUniquePropertiesIncludedInDiff) {
                linksToApplyDiffTo.push(link)
            }
        }
        return linksToApplyDiffTo
    }    
}

export default ApplyEventService