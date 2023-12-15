import { constants } from '../constants'

export const chainNamespaces = {
    ETHEREUM: 'eth',
    GOERLI: 'goerli',
    POLYGON: 'polygon',
    MUMBAI: 'mumbai',
    BASE: 'base',
    OPTIMISM: 'optimism',
    ARBITRUM: 'arbitrum',
    ARBITRUM_SEPOLIA: 'arbitrumsepolia',
    PGN: 'pgn',
    CELO: 'celo',
    LINEA: 'linea',
    SEPOLIA: 'sepolia',
}

export const TOKENS_NSP = 'tokens'

export const chainNamespacesSet = new Set(Object.values(chainNamespaces))

export const isContractNamespace = (nsp: string): boolean => nsp.includes('.')

export const isPrimitiveNamespace = (nsp: string): boolean =>
    nsp === TOKENS_NSP || chainNamespacesSet.has(nsp) || nsp === constants.SPEC
