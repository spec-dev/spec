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
        await this.getOps()
        await this.runOps()
    }

    async getOps(): Promise<Op[]> {
        // Get all links this diff applies to (i.e. the links 
        // who have all of their property keys included in this diff).
        this.linksToApplyDiffTo = this._getLinksToApplyDiffTo()
        if (!this.linksToApplyDiffTo.length) {
            throw 'Live object diff didn\'t satisfy any configured links'
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
            let diffHasAllRequiredLinkProperties = true
            for (const property in link.properties) {
                if (!liveObjectDiff.hasOwnProperty(property)) {
                    diffHasAllRequiredLinkProperties = false
                    break
                }
            }
            if (diffHasAllRequiredLinkProperties) {
                linksToApplyDiffTo.push(link)
            }
        }
        return linksToApplyDiffTo
    }    
}

export default ApplyEventService