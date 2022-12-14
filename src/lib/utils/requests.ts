import { StringKeyMap, EdgeFunction } from '../types'
import fetch, { Response } from 'node-fetch'
import { JSONParser } from '@streamparser/json'
import constants from '../constants'
import logger from '../logger'

type onDataCallbackType = (data: StringKeyMap | StringKeyMap[]) => Promise<void>

const isStreamingResp = (resp: Response): boolean =>
    resp.headers?.get('Transfer-Encoding') === 'chunked'

export async function callSpecFunction(
    edgeFunction: EdgeFunction,
    payload: StringKeyMap | StringKeyMap[],
    onData: onDataCallbackType,
    sharedErrorContext: StringKeyMap,
    hasRetried?: boolean
) {
    const abortController = new AbortController()
    const initialRequestTimer = setTimeout(() => abortController.abort(), 60000)
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
    sharedErrorContext: StringKeyMap
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
        chunkTimer = setTimeout(() => abortController.abort(), 60000)
    }
    renewTimer()

    let pendingDataPromise = null

    // Parse each JSON object and add it to a batch.
    let batch = []
    jsonParser.onValue = (obj) => {
        if (!obj) return
        obj = obj as StringKeyMap
        if (obj.error) throw obj.error // Throw any errors explicitly passed back

        batch.push(obj)
        if (batch.length === constants.STREAMING_SEED_UPSERT_BATCH_SIZE) {
            pendingDataPromise = onData([...batch])
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
): Promise<Response> {
    payload = stringifyAnyDates(payload)

    let resp: Response
    try {
        resp = await fetch(edgeFunction.url, {
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
        return value.map((v) => stringifyAnyDates(v))
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
