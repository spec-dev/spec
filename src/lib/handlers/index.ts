import { dynamicImport } from '../utils/imports'
import { chainNamespaces } from '../utils/chains'
import { constants } from '../constants'
import path from 'path'
import { fileExists } from '../utils/file'
import logger from '../logger'

let handlers = {}

export const CONTRACTS_EVENT_NSP = 'contracts'
export const CUSTOM_EVENT_HANDLER_KEY = 'ceh'

export async function importHandlers() {
    try {
        const handlersDir = path.join(constants.SPEC_CONFIG_DIR, 'handlers')
        if (!fileExists(handlersDir)) {
            handlers = {}
            return
        }

        // Import the default exported event handlers.
        const givenHandlers = (await dynamicImport(handlersDir)).default || {}
        if (!Object.keys(givenHandlers).length) {
            handlers = {}
            return
        }

        // Modify the event names to subscribe to if they either...
        // a) are a contract event and don't have a chain-specific namespace
        // b) don't have a version
        const resolvedHandlers = {}
        for (const givenEventName in givenHandlers) {
            let eventName = givenEventName
            const handler = givenHandlers[eventName]

            // Add a missing "contracts." prefix if missing.
            if (eventName.split('.').length === 3) {
                eventName = `${CONTRACTS_EVENT_NSP}.${eventName}`
            }

            // Add default version if it doesn't exist.
            if (!eventName.includes('@')) {
                eventName += '@0.0.1'
            }

            // Subscribe to contract event on all chains if chain not specified.
            if (eventName.startsWith(`${CONTRACTS_EVENT_NSP}.`)) {
                for (const nsp of Object.values(chainNamespaces)) {
                    resolvedHandlers[[nsp, eventName].join('.')] = handler
                }
            } else {
                resolvedHandlers[eventName] = handler
            }
        }
        handlers = resolvedHandlers
    } catch (err) {
        logger.error(err)
        handlers = {}
    }
}

export const getHandlers = () => handlers
