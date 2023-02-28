import { LiveObject } from '../types'
import messageClient from './messageClient'
import config from '../config'

export async function resolveLiveObjects(
    liveObjectIdsInConfig: string[],
    existingLiveObjects: { [key: string]: LiveObject }
): Promise<LiveObject[]> {
    // Find and resolve new live object entries in the config.
    const newLiveObjectIds = liveObjectIdsInConfig.filter(
        (id) => !existingLiveObjects.hasOwnProperty(id)
    )
    let newLiveObjects = {}
    if (newLiveObjectIds.length) {
        const { data, error } = await messageClient.resolveLiveObjects(newLiveObjectIds)
        if (error) return Object.values(existingLiveObjects)
        data.forEach((entry) => {
            newLiveObjects[entry.id] = entry
        })
    }

    // Refresh all live objects regardless of whether they're new,
    // as "links" could change in the config for an existing live object.
    const liveObjectsMapFromConfig = config.liveObjectsMap
    const liveObjects = []
    for (const id of liveObjectIdsInConfig) {
        const liveObject = liveObjectsMapFromConfig[id]
        const resolvedLiveObject = existingLiveObjects[id] || newLiveObjects[id]
        if (!resolvedLiveObject) continue
        const { events, edgeFunctions, config } = resolvedLiveObject
        liveObjects.push({
            ...liveObject,
            events,
            edgeFunctions,
            config,
        })
    }

    return liveObjects
}
