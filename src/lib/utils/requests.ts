
import { StringKeyMap, EdgeFunction } from '../types'
import fetch, { Response } from 'node-fetch'
import { JSONParser } from '@streamparser/json'
import constants from '../constants'

type onDataCallbackType = (data: StringKeyMap | StringKeyMap[]) => Promise<void>

const isStreamingResp = (resp: Response): boolean => resp.headers?.get('Transfer-Encoding') === 'chunked'

export async function callSpecFunction(
    edgeFunction: EdgeFunction, 
    payload: StringKeyMap | StringKeyMap[],
    onData: onDataCallbackType,
) {
    const resp = await makeRequest(edgeFunction, payload)

    await (isStreamingResp(resp)
        ? handleStreamingResp(resp, onData)
        : handleJSONResp(resp, edgeFunction, onData))
}

async function handleJSONResp(resp: Response, edgeFunction: EdgeFunction, onData: onDataCallbackType) {
    let data
    try {
        data = await resp.json()
    } catch (err) {
        throw `Failed to parse JSON response data from edge function ${edgeFunction.name}: ${err?.message || err}`
    }
    await onData(data)
}

async function handleStreamingResp(resp: Response, onData: onDataCallbackType) {
    // Create JSON parser for streamed response.
    const jsonParser = new JSONParser({
        stringBufferSize: undefined,
        paths: ['$.*'],
        keepStack: false,
    })

    // Parse each JSON object and add it to a batch.
    let batch = []
    let promises = []
    jsonParser.onValue = obj => {
        if (!obj) return
        obj = obj as StringKeyMap
        if (obj.error) throw obj.error // Throw any errors explicitly passed back
        batch.push(obj)
        if (batch.length === constants.STREAMING_SEED_UPSERT_BATCH_SIZE) {
            promises.push(onData([...batch]))
            batch = []
        }
    }

    let chunk
    try {
        for await (chunk of resp.body) {
            jsonParser.write(chunk)
        }
    } catch (err) {
        throw `Error iterating response stream on chunk ${chunk.toString()}: ${err?.message || err}`
    }
    
    // Trailing results in partial batch.
    if (batch.length) {
        promises.push(onData(batch))
    }

    await Promise.all(promises)
}

async function makeRequest(edgeFunction: EdgeFunction, payload: StringKeyMap | StringKeyMap[]): Response {
    let resp: Response
    try {
        resp = await fetch(edgeFunction.url, {
            method: 'POST',
            body: JSON.stringify(payload || {}),
            headers: { 'Content-Type': 'application/json' },
        })
    } catch (err) {
        throw `Unexpected error calling edge function ${edgeFunction.name}: ${err?.message || err}`
    }
    if (resp.status !== 200) {
        throw `Edge function (${edgeFunction.name}) call failed: got response code ${resp.status}`
    }
    return resp
}