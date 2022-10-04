import { StringKeyMap, EdgeFunction } from '../types'
import fetch, { Response } from 'node-fetch'
import { JSONParser } from '@streamparser/json'
import constants from '../constants'
import logger from '../logger'
import { db } from '../db'
import { camelizeKeys } from 'humps'

type onDataCallbackType = (data: StringKeyMap | StringKeyMap[]) => Promise<void>

// const isStreamingResp = (resp: Response): boolean =>
//     resp.headers?.get('Transfer-Encoding') === 'chunked'
const isStreamingResp = (resp: Response) => true

export async function callSpecFunction(
    edgeFunction: EdgeFunction,
    payload: StringKeyMap | StringKeyMap[],
    onData: onDataCallbackType,
    sharedErrorContext: StringKeyMap,
    hasRetried?: boolean
) {
    const abortController = new AbortController()
    const initialRequestTimer = setTimeout(() => abortController.abort(), 30000)
    const resp = await makeRequest(edgeFunction, payload, abortController)
    clearTimeout(initialRequestTimer)

    if (!isStreamingResp(resp)) {
        await handleJSONResp(resp, edgeFunction, onData)
        return
    }

    try {
        await handleStreamingResp(resp, abortController, onData, sharedErrorContext)
    } catch (err) {
        const message = err.message || err || ''
        if (!hasRetried && message.toLowerCase().includes('user aborted')) {
            logger.warn('Retrying spec function request...')
            await callSpecFunction(edgeFunction, payload, onData, sharedErrorContext, true)
        } else {
            throw err
        }
    }
}

async function handleJSONResp(
    resp: Response,
    edgeFunction: EdgeFunction,
    onData: onDataCallbackType
) {
    let data
    try {
        data = await resp.json()
    } catch (err) {
        throw `Failed to parse JSON response data from edge function ${edgeFunction.name}: ${
            err?.message || err
        }`
    }
    await onData(data)
}

async function handleStreamingResp(
    resp: Response,
    abortController: AbortController,
    onData: onDataCallbackType,
    sharedErrorContext: StringKeyMap,
) {
    // Create JSON parser for streamed response.
    const jsonParser = new JSONParser({
        stringBufferSize: undefined,
        paths: ['$.*'],
        keepStack: false,
    })

    let chunkTimer = null
    const renewTimer = () => {
        chunkTimer && clearTimeout(chunkTimer)
        chunkTimer = setTimeout(() => abortController.abort(), 300000000)
    }
    renewTimer()

    let pendingDataPromise = null

    // Parse each JSON object and add it to a batch.
    let batch = []
    // let promises = []
    jsonParser.onValue = (obj) => {
        if (!obj) return
        obj = obj as StringKeyMap
        if (obj.error) throw obj.error // Throw any errors explicitly passed back
        obj = camelizeKeys(obj)

        batch.push(obj)
        if (batch.length === constants.STREAMING_SEED_UPSERT_BATCH_SIZE) {
            pendingDataPromise = onData([...batch])
            // promises.push(onData([...batch]))
            batch = []
        }
    }

    let chunk
    try {
        for await (chunk of resp.body) {
            renewTimer()
            if (sharedErrorContext.error) {
                throw `Error handling streaming response batch: ${sharedErrorContext.error}`
            }

            if (pendingDataPromise) {
                await pendingDataPromise
                pendingDataPromise = null
            }

            jsonParser.write(chunk)
        }
    } catch (err) {
        chunkTimer && clearTimeout(chunkTimer)
        throw `Error iterating response stream: ${err?.message || err}`
    }
    chunkTimer && clearTimeout(chunkTimer)

    if (batch.length) {
        await onData([...batch])
    }
}

async function makeRequest(
    edgeFunction: EdgeFunction,
    payload: StringKeyMap | StringKeyMap[],
    abortController: AbortController
): Response {
    payload = hackPayload(edgeFunction.url, stringifyAnyDates(payload))

    let resp: Response
    try {
        resp = await fetch('https://tables-api.spec.dev/stream', {
            method: 'POST',
            body: JSON.stringify(payload || {}),
            headers: { 'Content-Type': 'application/json' },
            signal: abortController.signal,
        })
    } catch (err) {
        throw `Unexpected error calling edge function ${edgeFunction.name}: ${err?.message || err}`
    }
    if (resp?.status !== 200) {
        throw `Edge function (${edgeFunction.name}) call failed: got response code ${resp?.status}`
    }
    return resp
}

function stringifyAnyDates(value: StringKeyMap | StringKeyMap[]): StringKeyMap | StringKeyMap[] {
    // Null.
    if (value === null) return null

    // Arrays.
    if (Array.isArray(value)) {
        return value.map(v => stringifyAnyDates(v))
    }

    // Objects.
    if (typeof value === 'object') {
        // Dates.
        if (Object.prototype.toString.call(value) === '[object Date]') {
            return value.toISOString()
        }

        // "Dicts".
        if (Object.prototype.toString.call(value) === '[object Object]') {
            const clone = {}
            for (const key in value) {
                clone[key] = stringifyAnyDates(value[key])
            }
            return clone
        }
        return value
    }

    // Other.
    return value
}

function hackPayload(url: string, payload: StringKeyMap): StringKeyMap {
    if (url.endsWith('eth.latestInteractions@0.0.1')) {
        const query = db
            .withSchema('ethereum')
            .from('latest_interactions')
            .whereIn('to', payload.to)
        return query.toSQL().toNative()
    }
    return {}
}