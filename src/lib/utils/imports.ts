export const dynamicImport = async path => {
    try {
        return await import(path)
    } catch (e) {
        throw `Unable to import module ${path}`
    }
}