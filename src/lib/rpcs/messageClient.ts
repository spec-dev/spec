import { createEventClient, SpecEventClient, EventCallback } from '@spec.dev/event-client'
import {
    EventCursor,
    MessageClientOptions,
    ResolvedLiveObject,
    StringKeyMap,
    MissedEventsCallback,
    ReorgEvent,
} from '../types'
import { constants } from '../constants'
import { noop, stringify } from '../utils/formatters'
import { RpcError } from '../errors'
import RPC from './functionNames'
import logger from '../logger'
import short from 'short-uuid'
import chalk from 'chalk'
import { sleep } from '../utils/time'

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
        opts?.temp || logger.info(chalk.green(`Subscribed to event ${eventName}`))
    }

    async off(eventName: string, opts?: StringKeyMap) {
        await this.client.off(eventName, opts)
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

    async getMostRecentBlockNumbers(): Promise<{
        data: StringKeyMap | null
        error: RpcError | null
    }> {
        return this.call(RPC.GetMostRecentBlockNumbers)
    }

    async fetchMissedEvents(cursors: EventCursor[], cb: MissedEventsCallback, onDone?: any) {
        if (!cursors.length) {
            onDone && (await onDone())
            return
        }

        // Subscribe to a unique, temporary channel to use for missed-event transfer.
        const channel = short.generate()
        const opts = { resolveVersion: false, temp: true }
        this.on(
            channel,
            async (data: any) => {
                if (data && typeof data === 'object' && !Array.isArray(data) && data.done) {
                    await this.off(channel, opts)
                    onDone && (await onDone())
                } else {
                    await cb(data)
                }
            },
            opts
        )
        await sleep(100)

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

    async validateReorg(reorgEvent: ReorgEvent): Promise<boolean> {
        const { data, error } = await this.call(RPC.ValidateReorg, reorgEvent)
        if (error) {
            logger.error(error)
            throw error
        }
        const isValid = !!data?.isValid
        if (!isValid) {
            logger.error(chalk.redBright(`Invalid reorg detected: ${stringify(reorgEvent)}`))
        }
        return isValid
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
