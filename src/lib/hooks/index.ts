import { dynamicImport } from '../utils/imports'
import { constants } from '../constants'
import path from 'path'
import { fileExists } from '../utils/file'

let hooks = {}

export async function importHooks() {
    try {
        const hooksDir = path.join(constants.SPEC_CONFIG_DIR, 'hooks')
        if (!fileExists(hooksDir)) {
            hooks = {}
            return
        }

        // Import the default exported hooks.
        const givenHooks = (await dynamicImport(hooksDir)).default || {}
        if (!Object.keys(givenHooks).length) {
            hooks = {}
            return
        }
    } catch (err) {
        hooks = {}
    }
}

export const getHooks = () => hooks
export const hooksExist = () => Object.keys(hooks).length > 0
