import { SelectOptions, StringKeyMap } from '../types'
import fetch, { Response } from 'node-fetch'
import { JSONParser } from '@streamparser/json'
import { constants } from '../constants'
import logger from '../logger'
import { stringify } from '../utils/formatters'
import { camelizeKeys } from 'humps'
import { sleep } from '../utils/time'

type onDataCallbackType = (data: StringKeyMap | StringKeyMap[]) => Promise<void>

const isStreamingResp = (resp: Response): boolean =>
    resp.headers?.get('Transfer-Encoding') === 'chunked'

const queryUrl = (() => {
    const url = new URL(constants.SHARED_TABLES_ORIGIN)
    url.pathname = '/stream'
    return url.toString()
})()

export async function querySharedTable(
    tablePath: string,
    payload: StringKeyMap | StringKeyMap[],
    onData: onDataCallbackType,
    sharedErrorContext: StringKeyMap,
    options: SelectOptions = {},
    nearHead: boolean = false,
    attempt: number = 1
) {
    const queryPayload: StringKeyMap = {
        table: tablePath,
        filters: stringifyAnyDates(payload),
        options,
    }
    if (nearHead) {
        queryPayload.nearHead = true
    }

    const abortController = new AbortController()
    const resp = await makeRequest(tablePath, queryPayload, abortController)

    if (!isStreamingResp(resp)) {
        await handleJSONResp(resp, tablePath, onData)
        return
    }

    try {
        await handleStreamingResp(resp, abortController, onData, sharedErrorContext)
    } catch (err) {
        logger.error(`Error handling streaming response from shared table ${tablePath}: ${err}`)
        if (attempt < constants.EXPO_BACKOFF_MAX_ATTEMPTS) {
            logger.warn(
                `Retrying with attempt ${attempt}/${constants.EXPO_BACKOFF_MAX_ATTEMPTS}...`
            )
            await sleep(constants.EXPO_BACKOFF_FACTOR ** attempt * constants.EXPO_BACKOFF_DELAY)
            await querySharedTable(
                tablePath,
                payload,
                onData,
                sharedErrorContext,
                options || {},
                nearHead,
                attempt + 1
            )
        } else {
            throw err
        }
    }
}

async function handleJSONResp(resp: Response, tablePath: string, onData: onDataCallbackType) {
    let data
    try {
        data = await resp.json()
    } catch (err) {
        throw `Failed to parse JSON response data while querying shared table ${tablePath}: ${
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
        chunkTimer = setTimeout(
            () => abortController.abort(),
            constants.SHARED_TABLES_READABLE_STREAM_TIMEOUT
        )
    }
    renewTimer()

    let pendingDataPromise = null

    // Parse each JSON object and add it to a batch.
    let batch = []
    jsonParser.onValue = (obj) => {
        if (!obj) return
        obj = obj as StringKeyMap
        if (obj.error) throw obj.error // Throw any errors explicitly passed back

        obj = camelizeKeys(obj)

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
    tablePath: string,
    payload: StringKeyMap | StringKeyMap[],
    abortController: AbortController,
    attempt: number = 1
): Promise<Response> {
    const initialRequestTimer = setTimeout(
        () => abortController.abort(),
        constants.SHARED_TABLES_INITIAL_REQUEST_TIMEOUT
    )

    const headers = { 'Content-Type': 'application/json' }
    if (constants.PROJECT_API_KEY) {
        headers[constants.SHARED_TABLES_AUTH_HEADER_NAME] = constants.PROJECT_API_KEY
    }

    let resp, error
    try {
        resp = await fetch(queryUrl, {
            method: 'POST',
            body: JSON.stringify(payload),
            headers,
            signal: abortController.signal,
        })
    } catch (err) {
        error = `Unexpected error querying shared table ${tablePath}: ${stringify(err)}`
    }
    if (!error && resp?.status !== 200) {
        error = `Querying shared table (${tablePath}) failed: got response code ${resp?.status}`
    }
    clearTimeout(initialRequestTimer)

    if (error) {
        logger.error(
            `Failed to make initial request while querying shared table ${tablePath}: ${error}.`
        )

        if (attempt < constants.EXPO_BACKOFF_MAX_ATTEMPTS) {
            logger.error(
                `Retrying with attempt ${attempt}/${constants.EXPO_BACKOFF_MAX_ATTEMPTS}...`
            )
            await sleep(constants.EXPO_BACKOFF_FACTOR ** attempt * constants.EXPO_BACKOFF_DELAY)
            return makeRequest(tablePath, payload, abortController, attempt + 1)
        } else {
            throw error
        }
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
