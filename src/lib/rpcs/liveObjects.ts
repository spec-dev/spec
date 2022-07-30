import { LiveObject } from '../types'
import messageClient from './messageClient'
import config from '../config'

export async function resolveLiveObjects(liveObjectVersionIds: string[]): Promise<LiveObject[] | null> {
    const { data, error } = await messageClient.resolveLiveObjects(liveObjectVersionIds)
    if (error) return null
    if (!data.length) return []

    const liveObjectsMap = config.liveObjectsMap
    const liveObjects = []
    for (let entry of data) {
        const liveObject = liveObjectsMap[entry.id]
        liveObjects.push({
            ...liveObject,
            events: entry.events,
        })
    }
    return liveObjects
}