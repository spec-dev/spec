
import { SpecFunctionResponse, StringKeyMap } from '../types'
import { fetch } from 'cross-fetch'
import constants from '../constants'

export async function callSpecFunction(identifier: string, payload: StringKeyMap): Promise<SpecFunctionResponse> {
    const url = new URL(constants.FUNCTIONS_ORIGIN)
    url.pathname = `/${identifier}`

    let resp: Response
    try {
        resp = await fetch(url.href, {
            method: 'POST', 
            body: JSON.stringify(payload || {}),
            headers: { 'Content-Type': 'application/json' },
        })
    } catch (err) {
        console.error(`Unexpected error calling edge function ${identifier}: ${err?.message || err}`)
        return { data: null, error: err?.message || err }
    }

    if (resp.status !== 200) {
        console.error(`Unexpected error calling edge function ${identifier}: got response code ${resp.status}`)
        return { data: null, error: `got response code ${resp.status}` }
    }

    let respData: { [key: string]: any } = {}
    try {
        respData = await resp.json()
    } catch (err) {
        console.error(`Unexpected error calling edge function -- Failed to parse JSON response data: ${err?.message || err}`)
        return { data: null, error: `JSON parse error ${err?.message || err}` }
    }

    return respData as SpecFunctionResponse
}