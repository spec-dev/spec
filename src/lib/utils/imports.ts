export const dynamicImport = async (path: string) => {
    try {
        return await import(path)
    } catch (e) {
        throw `Unable to import module ${path}`
    }
}
