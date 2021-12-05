import test from 'ava'

import { TableData, handledPool, Table } from '..'
import * as rt from 'runtypes'

const Human = TableData.extend({
  name: rt.String, 
})
type Human = rt.Static<typeof Human>

test('It should process commands', async (t) => {
  const pool = await handledPool()
  const table = new Table<Human>( Human.check, pool, 'human' )
  await table.init()
  const rec = await table.put({_id:'human:bob',name:'Bob'})
  const rec2 = await table.put({...rec,name:'Jenny'})
  await table.get('human:bob')
  await table.delete(rec2)
  await pool.end()
  t.pass()
})
