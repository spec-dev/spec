
import { SpecFunctionResponse, StringKeyMap, EdgeFunction } from '../types'
import { fetch } from 'cross-fetch'
import logger from '../logger'
import constants from '../constants'

export async function callSpecFunction(edgeFunction: EdgeFunction, payload: StringKeyMap | StringKeyMap[]): Promise<SpecFunctionResponse> {
    let resp: Response
    try {
        resp = await fetch(edgeFunction.url, {
            method: 'POST', 
            body: JSON.stringify(payload || {}),
            headers: { 'Content-Type': 'application/json' },
        })
    } catch (err) {
        logger.error(`Unexpected error calling edge function ${edgeFunction.name}: ${err?.message || err}`)
        return { data: null, error: err?.message || err }
    }

    if (resp.status !== 200) {
        logger.error(`Unexpected error calling edge function ${edgeFunction.name}: got response code ${resp.status}`)
        return { data: null, error: `got response code ${resp.status}` }
    }

    let respData: { [key: string]: any } = {}
    try {
        respData = await resp.json()
    } catch (err) {
        logger.error(`Unexpected error calling edge function -- Failed to parse JSON response data: ${err?.message || err}`)
        return { data: null, error: `JSON parse error ${err?.message || err}` }
    }

    return respData as SpecFunctionResponse
}