import { LiveObject } from '../types'
import messageClient from './messageClient'
import config from '../config'

export async function resolveLiveObjects(liveObjectIds: string[]): Promise<LiveObject[] | null> {
    const { data, error } = await messageClient.resolveLiveObjects(liveObjectIds)
    if (error) return null
    if (!data.length) return []

    const liveObjectsMap = config.liveObjectsMap
    const liveObjects = []
    for (let entry of data) {
        const {
            id,
            events,
            edgeFunctions,
            config,
        } = entry

        const liveObject = liveObjectsMap[id]

        liveObjects.push({
            ...liveObject,
            events,
            edgeFunctions,
            config,
        })
    }
    return liveObjects
}
