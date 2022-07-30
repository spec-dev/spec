import logger from './lib/logger'
import Spec from './spec'

function run() {
    logger.info('Starting Spec...')
    const spec = new Spec()
    spec.start()
}

run()