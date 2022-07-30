import { createEventClient, SpecEventClient, EventCallback } from '@spec.dev/event-client'
import { EventCursor, MessageClientOptions } from '../types'
import constants from '../constants'
import { noop } from '../utils/formatters'
import { RpcError } from '../errors'
import RPC from './functionNames'
import logger from '../logger'
import short from 'short-uuid'

const DEFAULT_OPTIONS = {
    onConnect: noop,
}

interface ResolvedLiveObject {
    id: string // i.e. "compound.CompoundMarketAPY@0.0.1"
    events: Event[]
}

export class MessageClient {

    client: SpecEventClient

    onConnect: () => void

    /**
     * Create a new MessageClient instance.
     */
    constructor(options?: MessageClientOptions) {
        const settings = { ...DEFAULT_OPTIONS, ...options }
        this.onConnect = settings.onConnect || noop
        this.client = null
    }

    connect() {
        this.client = createEventClient({
            hostname: constants.EVENTS_HOSTNAME,
            port: constants.EVENTS_PORT,
            onConnect: this.onConnect,
        })
    }

    on(eventName: string, cb: EventCallback) {
        this.client.on(eventName, cb)
    }

    async off(eventName: string) {
        await this.client.off(eventName)
    }

    async resolveLiveObjects(liveObjectVersionIds: string[]): Promise<{ 
        data: ResolvedLiveObject[] | null, 
        error: RpcError | null,
    }> {
        const { data, error } = await this.call(RPC.ResolveLiveObjects, { 
            ids: liveObjectVersionIds,
        })
        return {
            data: data ? data as ResolvedLiveObject[] : null,
            error,
        }
    }

    async fetchMissedEvents(cursors: EventCursor[], cb: EventCallback) {
        // Subscribe to a unique, temporary channel to use for missed-event transfer.
        const channel = short.generate()
        this.on(channel, cb)

        // Tell the server to find and send over the missed events.
        const { error } = await this.call(RPC.GetEventsAfterCursors, {
            cursors,
            channel,
        })
        if (error) {
            logger.error(error)
            throw error
        }

        // Trash temp channel.
        await this.off(channel)
    }

    async call(functionName: RPC, payload?: any): Promise<{ data: any, error: RpcError }> {
        let data = null
        try {
            data = await this.client.socket.invoke(functionName, payload)
        } catch (err) {
            const error = new RpcError(functionName, err)
            logger.error(error)
            return { data: null, error }
        }
        return { data, error: null }
    }
}

const messageClient = new MessageClient()
export default messageClient