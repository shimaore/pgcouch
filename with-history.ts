export class TableWithHistory<T> extends Table<T> {
  protected readonly historyName : string
  constructor(
    check: (x:any) => T,
    pool:PoolType,
    name:TableName,
  ) {
    super(check,pool,name)
    this.historyName = `${this.name}  History`
  }
  async init() {
    await super.init()
    const { historyName } = this
    const client = await this.pool.connect()
    // Returns an array with 4 `result`
    const historyTable = [
      `CREATE TABLE "${historyName}" ( data JSONB NOT NULL );`,
      `CREATE UNIQUE INDEX "${historyName} _id_rev" ON "${historyName}" ((data->'_id'),(data->'_rev'));`,
      `CREATE INDEX "${historyName} gin" ON "${historyName}" USING GIN(data jsonb_path_ops);`,
      `CREATE INDEX "${historyName} btree" ON "${historyName}" USING BTREE(data);`,
    ] : []
    for (const q of historyTable) {
      try {
        const res = await client.query(q)
        logger.info({q,res},'init')
      } catch (err) {
        logger.info({q,err},'init')
      }
    }
    client.release()
  }

  async get(_id:DocumentId,_rev?:Revision) : Promise<T | undefined> {
    DocumentId.check(_id)
    const query = _rev ? {_id,_rev} : {_id}
    const stream = await this.query(query)
    // return the first value
    const res = await stream.take(1).last()
    if(res) {
      return this.check(res)
    } else {
      return undefined
    }
  }

  // FIXME implement put
  // FIXME implement delete

  async query(query:string|object) : Promise<LakeAsyncIterator<T>> {
    const { tableName, historyName } = this

    const selectBase = typeof query === 'string'
      ? `SELECT data FROM "${tableName}" WHERE data @@ $1`
      : `SELECT data FROM "${tableName}" WHERE data @> $1`
    const selectHistory = typeof query === 'string'
      ? `SELECT data FROM "${historyName}" WHERE data @@ $1`
      : `SELECT data FROM "${historyName}" WHERE data @> $1`

    // PostgreSQL will merge identical JSONB records on UNION.
    const queryStream = new QueryStream(
      this.history ? `${selectBase} UNION ${selectHistory}` : selectBase,
      [ query ]
    )
    const lake = await buildLake(queryStream,this.pool)
    return lake.map( (data:unknown) => this.check(data) )
  }

}
