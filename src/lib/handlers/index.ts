import { dynamicImport } from '../utils/imports'
import { constants } from '../constants'
import path from 'path'
import { fileExists } from '../utils/file'
import logger from '../logger'

let handlers = {}

export async function importHandlers() {
    try {
        const handlersDir = path.join(constants.SPEC_CONFIG_DIR, 'handlers')
        if (!fileExists(handlersDir)) return
        handlers = (await dynamicImport(handlersDir)).default || {}
    } catch (err) {
        logger.error(err)
        handlers = {}
    }
}

export const getHandlers = () => handlers
