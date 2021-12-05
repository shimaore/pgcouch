import Pg from 'pg';
const { Pool } = Pg;
import QueryStream from 'pg-query-stream';
import { from } from '@shimaore/lake';
import Pino from 'pino';
import * as rt from 'runtypes';
import { createHash } from 'crypto';
const logger = Pino({ name: '@shimaore/pgcouch' });
export const handledPool = async () => {
    const pool = new Pool();
    pool.on('error', (error) => logger.error({ error }, 'pool.error'));
    return pool;
};
// runtype TableName ensures the name is a valid (e.g. CouchDB) name.
export const TableName = rt.String; // .withConstraint( x => s.match(//) )
// runtype TableData contains at least a field named `_id`
export const DocumentId = rt.String; // .withConstraint etc.
export const Revision = rt.String.withBrand('Revision');
export const TableData = rt.Record({
    _id: DocumentId,
    _rev: Revision.optional(),
});
const revision = (a) => {
    const hash = createHash('sha256');
    hash.update(JSON.stringify(a));
    return hash.digest('hex');
};
export class Table {
    constructor(check, pool, name) {
        this.check = check;
        this.pool = pool;
        this.name = name;
        this.ready = false;
    }
    async init() {
        const { name } = this;
        const client = await this.pool.connect();
        // Returns an array with 4 `result`
        const res = await client.query(`
      CREATE TABLE IF NOT EXISTS "${name}" ( data JSONB );
      CREATE UNIQUE INDEX ON "${name}" ((data -> '_id'));
      CREATE INDEX ON "${name}" USING GIN(data jsonb_path_ops);
      CREATE INDEX ON "${name}" USING BTREE(data);
    `);
        client.release();
        logger.info({ res }, 'init');
    }
    async put(data) {
        const { name } = this;
        const client = await this.pool.connect();
        // FIXME: _id MUST be provided
        const _rev = revision(data);
        const finalData = { ...data, _rev };
        if (data._rev) {
            // FIXME: check something was modified
            const res = await client.query(`
        UPDATE "${name}"  SET data = $1 WHERE data @> $2
      `, [
                finalData,
                { _id: data._id, _rev: data._rev },
            ]);
            logger.info({ res }, 'put');
            client.release();
            return finalData;
        }
        else {
            // FIXME check something was inserted
            const res = await client.query(`
        INSERT INTO "${name}"(data) VALUES ($1)
      `, [
                finalData,
            ]);
            logger.info({ res }, 'put');
            client.release();
            return finalData;
        }
    }
    async get(_id) {
        const stream = await this.query({ _id });
        // return the first value
        const res = await stream.take(1).last();
        return res;
    }
    async delete(data) {
        const { name } = this;
        const client = await this.pool.connect();
        // FIXME ensure `_rev` is present
        const res = await client.query(`
      DELETE FROM "${name}" WHERE data @> $1
    `, [
            { _id: data._id, _rev: data._rev },
        ]);
        client.release();
        logger.info({ res }, 'delete');
        if (res.rowCount !== 1) {
            throw new Error(`Missing`);
        }
    }
    // MongoDB-style query = { _id: 'user:bob' }
    // JSONPatch query = '$.year > 1989'
    async query(query) {
        const { name } = this;
        const queryStream = new QueryStream(typeof query === 'string'
            ? `SELECT data FROM "${name}" WHERE data @@ $1`
            : `SELECT data FROM "${name}" WHERE data @> $1`, [query]);
        const stream = await new Promise((resolve, reject) => {
            this.pool.connect((err, client, done) => {
                if (err) {
                    reject(err);
                    return;
                }
                const stream = client.query(queryStream);
                stream.on('end', done);
                resolve(stream);
            });
        });
        return from(stream).map(({ data }) => this.check(data));
    }
}
