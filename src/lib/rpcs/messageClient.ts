import { createEventClient, SpecEventClient, EventCallback } from '@spec.dev/event-client'
import { EventCursor, MessageClientOptions, ResolvedLiveObject, StringKeyMap } from '../types'
import constants from '../constants'
import { noop } from '../utils/formatters'
import { RpcError } from '../errors'
import RPC from './functionNames'
import logger from '../logger'
import short from 'short-uuid'

const DEFAULT_OPTIONS = {
    onConnect: noop,
}

export class MessageClient {
    client: SpecEventClient

    onConnect: () => void

    pingJob: any = null

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
            signedAuthToken: constants.PROJECT_API_KEY,
            ackTimeout: 30000,
            onConnect: () => {
                this._createPingJobIfNotExists()
                this.onConnect()
            },
        })
    }

    on(eventName: string, cb: EventCallback, opts?: StringKeyMap) {
        this.client.on(eventName, cb, opts)
    }

    async off(eventName: string) {
        await this.client.off(eventName)
    }

    async resolveLiveObjects(liveObjectIds: string[]): Promise<{
        data: ResolvedLiveObject[] | null
        error: RpcError | null
    }> {
        const { data, error } = await this.call(RPC.ResolveLiveObjects, {
            ids: liveObjectIds,
        })
        return {
            data: data ? (data as ResolvedLiveObject[]) : null,
            error,
        }
    }

    async fetchMissedEvents(cursors: EventCursor[], cb: EventCallback, onDone?: any) {
        // Subscribe to a unique, temporary channel to use for missed-event transfer.
        const channel = short.generate()
        this.on(
            channel,
            async (data: any) => {
                if (data && typeof data === 'object' && !Array.isArray(data) && data.done) {
                    await this.off(channel)
                    onDone && onDone()
                } else {
                    cb(data)
                }
            },
            { temp: true }
        )

        // Tell the server to find and send over the missed events.
        const { error } = await this.call(RPC.GetEventsAfterCursors, {
            cursors,
            channel,
        })
        if (error) {
            logger.error(error)
            throw error
        }
    }

    async call(functionName: RPC, payload?: any): Promise<{ data: any; error: RpcError }> {
        let data = null
        try {
            data = await this.client.socket.invoke(functionName, payload)
        } catch (err) {
            const error = new RpcError(functionName, err)
            if (functionName !== RPC.Ping) {
                logger.error(error.message)
            }
            return { data: null, error }
        }
        return { data, error: null }
    }

    _createPingJobIfNotExists() {
        this.pingJob =
            this.pingJob ||
            setInterval(() => this.call(RPC.Ping, { ping: true }), constants.EVENTS_PING_INTERVAL)
    }
}

const messageClient = new MessageClient()
export default messageClient
