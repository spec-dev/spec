import Spec from './spec'
new Spec().start()
// import { db } from './lib/db'
// import { getTableConstraints } from './lib/db/tablesMeta'
// import { ConstraintType } from './lib/types'
// import { doesTableExist } from './lib/db/ops'

// async function run() {
    // Get all table constraints.
    // const constraints = await getTableConstraints('public.Address_segment_ref')
//     const colName = 'contract_addresses'
//     const records = await db
//         .from('public.Segments')
//         .select('*')
//         .whereRaw('? = ANY(??)', ['0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9', colName])
//     console.log(records)
// }

// run()