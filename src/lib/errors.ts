import RPC from '../lib/rpcs/functionNames'

export class ConfigError extends Error {
    constructor(errorOrMessage: any) {
        const name = 'ConfigError'
        super(`${errorOrMessage?.message || errorOrMessage}.`)
        this.name = name
    }
}

export class RpcError extends Error {
    constructor(functionName: RPC, errorOrMessage: any) {
        const name = 'RPC Error'
        super(`Error invoking ${functionName} rpc: ${errorOrMessage?.message || errorOrMessage}.`)
        this.name = name
    }
}

export class QueryError extends Error {
    constructor(op: string, schema: string, table: string, errorOrMessage: any) {
        const name = 'Query Error'
        super(
            `(op=${op}; schema=${schema} table=${table}: ${
                errorOrMessage?.message || errorOrMessage
            }`
        )
        this.name = name
    }
}
