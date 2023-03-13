import { dynamicImport } from '../utils/imports'
import { chainNamespaces } from '../utils/chains'

let handlers = {}

export const CONTRACTS_EVENT_NSP = 'contracts'
export const CUSTOM_EVENT_HANDLER_KEY = 'ceh'

export async function importHandlers() {
    try {
        // Import the default exported event handlers.
        const givenHandlers = (await dynamicImport('@spec.custom/handlers')).default || {}
        if (!Object.keys(givenHandlers).length) {
            handlers = givenHandlers
            return
        }

        // Modify the event names to subscribe to if they either...
        // a) are a contract event and don't have a chain-specific namespace
        // b) don't have a version
        const resolvedHandlers = {}
        for (const eventName in givenHandlers) {
            const handler = givenHandlers[eventName]

            // Add default version if it doesn't exist.
            let resolvedEventName = eventName
            if (!resolvedEventName.includes('@')) {
                resolvedEventName += '@0.0.1'
            }

            // Subscribe to contract event on all chains if chain not specified.
            if (resolvedEventName.startsWith(`${CONTRACTS_EVENT_NSP}.`)) {
                for (const nsp of Object.values(chainNamespaces)) {
                    resolvedHandlers[[nsp, resolvedEventName].join('.')] = handler
                }
            } else {
                resolvedHandlers[resolvedEventName] = handler
            }
        }
        handlers = resolvedHandlers
    } catch (err) {
        handlers = {}
    }
}

export const getHandlers = () => handlers
