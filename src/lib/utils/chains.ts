export const chainNamespaces = {
    ETHEREUM: 'eth',
    GOERLI: 'goerli',
    POLYGON: 'polygon',
    MUMBAI: 'mumbai',
    BASE: 'base',
}

export const TOKENS_NSP = 'tokens'

export const chainNamespacesSet = new Set(Object.values(chainNamespaces))

export const isContractNamespace = (nsp: string): boolean => {
    const splitNsp = (nsp || '').split('.')
    return Object.values(chainNamespaces).includes(splitNsp[0]) && splitNsp[1] === 'contracts'
}

export const isPrimitiveNamespace = (nsp: string): boolean =>
    nsp === TOKENS_NSP || chainNamespacesSet.has(nsp)
