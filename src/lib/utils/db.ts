import { sleep } from './time'
import { randomIntegerInRange } from './math'

export async function withDeadlockProtection(fn: Function, attempt: number = 1) {
    try {
        await fn()
    } catch (err) {
        const message = err.message || err.toString() || ''
        if (attempt <= 3 && message.toLowerCase().includes('deadlock')) {
            await sleep(randomIntegerInRange(50, 100))
            await withDeadlockProtection(fn, attempt + 1)
        } else {
            throw err
        }
    }
}
