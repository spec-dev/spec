import { dynamicImport } from '../utils/imports'

let hooks = {}

export async function importHooks() {
    try {
        hooks = (await dynamicImport('@spec.custom/hooks')).default || {}
    } catch (err) {
        hooks = {}
    }
}

export const getHooks = () => hooks
export const hooksExist = () => Object.keys(hooks).length > 0
