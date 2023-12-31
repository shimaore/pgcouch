import { Pool } from 'pg'
import QueryStream from 'pg-query-stream'
import { LakeAsyncIterator, from } from '@shimaore/lake'
import { Contract } from 'runtypes'
import * as rt from 'runtypes'
import { createHash } from 'crypto'

/** TableName — ensures the name is a valid (e.g. CouchDB) name.
 */
export const TableName = rt.String
// .withConstraint( s => !!s.match(/^[a-z_][a-z0-9]*$/) )
export type TableName = rt.Static<typeof TableName>

/** TableData — contains at least a field named `_id`
 */

export const DocumentId = rt.String
  .withConstraint( s => s.length > 0 || 'document id must not be the empty string' )
export type DocumentId = rt.Static<typeof DocumentId>

export const Revision = rt.String
  .withBrand('Revision')
  .withConstraint( s => s.length > 0 || 'revision must not be the empty string')
export type Revision = rt.Static<typeof Revision>

export const TableData = rt.Record({
  _id: DocumentId,
  _rev: Revision.optional(),
})
export type TableData = rt.Static<typeof TableData>

/**
 * revision — computes a revision from the content of the record
 */
const revision = Contract(
  // Parameters
  TableData,
  // Return type
  Revision,
).enforce( (a:TableData) => {
  const hash = createHash('sha256')
  hash.update(JSON.stringify(a))
  return Revision.check(hash.digest('hex'))
})

export class TableError extends Error {
  constructor( msg: string, private readonly key: object ) {
    super(msg)
  }
  toJSON() { return {
    message: super.toString(),
    key: this.key,
  }}
}

export class TableMissingOnUpdateError extends TableError {}
export class TableConflictOnUpdateError extends TableError {}
export class TableMissingOnDeleteError extends TableError {}

export class Table<T extends TableData> {
  protected readonly tableName : string
  constructor(
    public readonly check: (x:any) => T,
    protected readonly pool:Pool,
    public readonly name:TableName,
  ) {
    TableName.check(this.name)
    this.tableName = this.name // or could be e.g. `${this.name}  Data`
  }

  /** init — Connection and idempotent table creation
   * Must be called before any other operation.
   *
   * @returns true if the table existed
   */
  async init() : Promise<boolean> {
    const { tableName } = this
    const client = await this.pool.connect()
    // Returns an array with 4 `result`
    const table = [
      `CREATE TABLE "${tableName}" ( data JSONB NOT NULL )`,
      // We store only one record per _id (we do not keep historical revisions,
      // since we are not planning to support master-master replication).
      `CREATE UNIQUE INDEX "${tableName} _id" ON "${tableName}" ((data->'_id'))`,
      `CREATE INDEX "${tableName} gin" ON "${tableName}" USING GIN(data jsonb_path_ops)`,
      `CREATE INDEX "${tableName} btree" ON "${tableName}" USING BTREE(data)`,
    ]
    try {
      await client.query('BEGIN')
      for (const q of table) {
        await client.query(q)
      }
      await client.query('COMMIT')
      return false
    } catch(err:any) {
      await client.query('ROLLBACK')
      const code = err?.code
      if(code === '42P07' || code === '23505') {
        return true
      } else {
        return Promise.reject(err)
      }
    } finally {
      client.release()
    }
  }

  async put(data:T) : Promise<T> {
    this.check(data)
    const { tableName } = this
    const client = await this.pool.connect()
    const _rev = revision(data)
    const finalData = this.check({ ...data, _rev })
    if(data._rev) {
      const key = { _id: data._id, _rev: data._rev }
      const res = await client.query(`
        UPDATE "${tableName}"  SET data = $1 WHERE data @> $2
      `, [ finalData, key ])
      client.release()
      if(res.rowCount !== 1) {
        // Missing, conflict, …
        return Promise.reject(new TableMissingOnUpdateError('Missing',key))
      }
      return finalData
    } else {
      const res = await client.query(`
        INSERT INTO "${tableName}"(data) VALUES ($1)
      `, [
        finalData, 
      ])
      client.release()
      if(res.rowCount !== 1) {
        // Conflict
        return Promise.reject(new TableConflictOnUpdateError('Invalid',data))
      }
      return finalData
    }
  }

  async get(_id:DocumentId) : Promise<T | undefined> {
    DocumentId.check(_id)
    const stream = await this.query({_id})
    // return the first value
    const res = await stream.take(1).last()
    if(res) {
      return this.check(res)
    } else {
      return undefined
    }
  }

  async delete(data:TableData) : Promise<void> {
    this.check(data)
    Revision.check(data._rev)
    const { tableName } = this
    const client = await this.pool.connect()
    const key = { _id: data._id, _rev: data._rev }
    const res = await client.query(`
      DELETE FROM "${tableName}" WHERE data @> $1
    `, [ key ])
    client.release()
    if(res.rowCount !== 1) {
      return Promise.reject(new TableMissingOnDeleteError(`Missing`,key))
    }
  }

  /**
   * `query` is a generic table query.
   * It can use:
   * - MongoDB-style query = { _id: 'user:bob' }, see
   *   [Containment](https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT)
   * - JSONPatch query = '$.year > 1989', see
   *   [JSON Path](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH)
   * It return an AsyncIterable with extended capabilities.
   */
  async query(query:string|object) : Promise<LakeAsyncIterator<T>> {
    const { tableName } = this

    const select = typeof query === 'string'
      ? `SELECT data FROM "${tableName}" WHERE data @@ $1`
      : `SELECT data FROM "${tableName}" WHERE data @> $1`

    // PostgreSQL will merge identical JSONB records on UNION.
    const queryStream = new QueryStream( select, [ query ] )
    const lake = await buildLake(queryStream,this.pool)
    return lake.map( (data:unknown) => this.check(data) )
  }
}

export const buildLake = async (queryStream:QueryStream,pool:Pool) : Promise<LakeAsyncIterator<unknown>> => {
  const stream : QueryStream = await new Promise( (resolve,reject) => {
      pool.connect( (err,client,done) => {
        if (err) {
          reject(err)
          return
        }
        if (!client) {
          reject(new Error('No client'))
          return
        }
        const stream : QueryStream = client.query(queryStream)
        stream.on('end',done)
        resolve(stream)
      })
    })
  return from(stream).map( ({data}) => data )
}
