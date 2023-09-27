export const chainNamespaces = {
    ETHEREUM: 'eth',
    GOERLI: 'goerli',
    POLYGON: 'polygon',
    MUMBAI: 'mumbai',
    BASE: 'base',
}

export const isContractNamespace = (nsp: string): boolean => {
    const splitNsp = (nsp || '').split('.')
    return Object.values(chainNamespaces).includes(splitNsp[0]) && splitNsp[1] === 'contracts'
}
