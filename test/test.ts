import test from 'ava'

import { TableData, handledPool, Table, TableConflictOnUpdateError } from '..'
import * as rt from 'runtypes'

const Human = TableData.extend({
  name: rt.String, 
})
type Human = rt.Static<typeof Human>

test('It should process commands', async (t) => {
  const pool = await handledPool()
  const table = new Table<Human>( Human.check, pool, 'human' )
  await table.init()
  let rec : Human
  try {
    rec = await table.put({_id:'human:bob',name:'Bob'})
  } catch (error) {
    if (error instanceof TableConflictOnUpdateError) {
      const val = await table.get('human:bob')
      if(!val) {
        t.fail('Conflict but no value')
        return
      }
      rec = val
    } else {
      t.fail(`${error}`)
      return
    }
  }
  const rec2 = await table.put({...rec,name:'Jenny'})
  await table.get('human:bob')
  await table.delete(rec2)
  await pool.end()
  t.pass()
})
test('Init() should be idempotent', async (t) => {
  const pool = await handledPool()
  const table = new Table<Human>( Human.check, pool, 'human' )
  await table.init()
  await table.init()
  await table.init()
  t.pass()
})
// test('It should process commands with history', async (t) => {
//   const pool = await handledPool()
//   const table = new Table<Human>( Human.check, pool, 'human', true )
//   await table.init()
//   const rec = await table.put({_id:'human:bob',name:'Bob'})
//   const rec2 = await table.put({...rec,name:'Jenny'})
//   await table.get('human:bob')
//   await table.delete(rec2)
//   await pool.end()
//   t.pass()
// })
