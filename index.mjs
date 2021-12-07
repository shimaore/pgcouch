import Pg from 'pg';
const { Pool } = Pg;
import QueryStream from 'pg-query-stream';
import { from } from '@shimaore/lake';
import Pino from 'pino';
import { Contract } from 'runtypes';
import * as rt from 'runtypes';
import { createHash } from 'crypto';
const logger = Pino({ name: '@shimaore/pgcouch' });
export const handledPool = async () => {
    const pool = new Pool();
    pool.on('error', (error) => logger.error({ error }, 'pool.error'));
    return pool;
};
// runtype TableName ensures the name is a valid (e.g. CouchDB) name.
export const TableName = rt.String;
// runtype TableData contains at least a field named `_id`
export const DocumentId = rt.String.withConstraint(s => s.length > 0 || 'document id must not be the empty string');
export const Revision = rt.String.withBrand('Revision').withConstraint(s => s.length > 0 || 'revision must not be the empty string');
export const TableData = rt.Record({
    _id: DocumentId,
    _rev: Revision.optional(),
});
const revision = Contract(
// Parameters
TableData, 
// Return type
Revision).enforce((a) => {
    const hash = createHash('sha256');
    hash.update(JSON.stringify(a));
    return Revision.check(hash.digest('hex'));
});
export class TableError extends Error {
}
export class Table {
    constructor(check, pool, name) {
        this.check = check;
        this.pool = pool;
        this.name = name;
        TableName.check(this.name);
        this.tableName = `${this.name}  Data`;
        this.logger = logger.child({ table: this.name });
    }
    async init() {
        const { tableName } = this;
        const client = await this.pool.connect();
        // Returns an array with 4 `result`
        const table = [
            `CREATE TABLE "${tableName}" ( data JSONB NOT NULL );`,
            `CREATE UNIQUE INDEX "${tableName} _id" ON "${tableName}" ((data->'_id'));`,
            `CREATE INDEX "${tableName} gin" ON "${tableName}" USING GIN(data jsonb_path_ops);`,
            `CREATE INDEX "${tableName} btree" ON "${tableName}" USING BTREE(data);`,
        ];
        for (const q of table) {
            try {
                const res = await client.query(q);
                this.logger.info({ q, res }, 'init');
            }
            catch (err) {
                if (err?.code === '42P07') {
                    this.logger.info({ q }, 'init: duplicate (ignored)');
                }
                else {
                    client.release();
                    throw err;
                }
            }
        }
        client.release();
    }
    async put(data) {
        this.check(data);
        const { tableName } = this;
        const client = await this.pool.connect();
        const _rev = revision(data);
        const finalData = this.check({ ...data, _rev });
        if (data._rev) {
            const res = await client.query(`
        UPDATE "${tableName}"  SET data = $1 WHERE data @> $2
      `, [
                finalData,
                { _id: data._id, _rev: data._rev },
            ]);
            if (res.rowCount !== 1) {
                throw new TableError('Missing');
            }
            this.logger.info({ res }, 'put');
            client.release();
            return finalData;
        }
        else {
            const res = await client.query(`
        INSERT INTO "${tableName}"(data) VALUES ($1)
      `, [
                finalData,
            ]);
            if (res.rowCount !== 1) {
                throw new TableError('Invalid');
            }
            this.logger.info({ res }, 'put');
            client.release();
            return finalData;
        }
    }
    async get(_id) {
        DocumentId.check(_id);
        const stream = await this.query({ _id });
        // return the first value
        const res = await stream.take(1).last();
        if (res) {
            return this.check(res);
        }
        else {
            return undefined;
        }
    }
    async delete(data) {
        this.check(data);
        Revision.check(data._rev);
        const { tableName } = this;
        const client = await this.pool.connect();
        const res = await client.query(`
      DELETE FROM "${tableName}" WHERE data @> $1
    `, [
            { _id: data._id, _rev: data._rev },
        ]);
        client.release();
        this.logger.info({ res }, 'delete');
        if (res.rowCount !== 1) {
            throw new TableError(`Missing`);
        }
    }
    /*
     * `query` is a generic table query.
     * It can use:
     * - MongoDB-style query = { _id: 'user:bob' }, see
     *   [Containment](https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT)
     * - JSONPatch query = '$.year > 1989', see
     *   [JSON Path](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH)
     *
     */
    async query(query) {
        const { tableName } = this;
        const select = typeof query === 'string'
            ? `SELECT data FROM "${tableName}" WHERE data @@ $1`
            : `SELECT data FROM "${tableName}" WHERE data @> $1`;
        // PostgreSQL will merge identical JSONB records on UNION.
        const queryStream = new QueryStream(select, [query]);
        const lake = await buildLake(queryStream, this.pool);
        return lake.map((data) => this.check(data));
    }
}
export const buildLake = async (queryStream, pool) => {
    const stream = await new Promise((resolve, reject) => {
        pool.connect((err, client, done) => {
            if (err) {
                reject(err);
                return;
            }
            const stream = client.query(queryStream);
            stream.on('end', done);
            resolve(stream);
        });
    });
    return from(stream).map(({ data }) => data);
};
