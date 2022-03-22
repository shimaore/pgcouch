var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from2, except, desc) => {
  if (from2 && typeof from2 === "object" || typeof from2 === "function") {
    for (let key of __getOwnPropNames(from2))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from2[key], enumerable: !(desc = __getOwnPropDesc(from2, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target, mod));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var pgcouch_exports = {};
__export(pgcouch_exports, {
  DocumentId: () => DocumentId,
  Revision: () => Revision,
  Table: () => Table,
  TableData: () => TableData,
  TableError: () => TableError,
  TableName: () => TableName,
  buildLake: () => buildLake,
  handledPool: () => handledPool
});
module.exports = __toCommonJS(pgcouch_exports);
var import_pg = __toESM(require("pg"), 1);
var import_pg_query_stream = __toESM(require("pg-query-stream"), 1);
var import_lake = require("@shimaore/lake");
var import_pino = __toESM(require("pino"), 1);
var import_runtypes = require("runtypes");
var rt = __toESM(require("runtypes"), 1);
var import_crypto = require("crypto");
const { Pool } = import_pg.default;
const logger = (0, import_pino.default)({ name: "@shimaore/pgcouch" });
const handledPool = async () => {
  const pool = new Pool();
  pool.on("error", (error) => logger.error({ error }, "pool.error"));
  return pool;
};
const TableName = rt.String;
const DocumentId = rt.String.withConstraint((s) => s.length > 0 || "document id must not be the empty string");
const Revision = rt.String.withBrand("Revision").withConstraint((s) => s.length > 0 || "revision must not be the empty string");
const TableData = rt.Record({
  _id: DocumentId,
  _rev: Revision.optional()
});
const revision = (0, import_runtypes.Contract)(TableData, Revision).enforce((a) => {
  const hash = (0, import_crypto.createHash)("sha256");
  hash.update(JSON.stringify(a));
  return Revision.check(hash.digest("hex"));
});
class TableError extends Error {
  constructor(msg, key) {
    super(msg);
    this.key = key;
  }
  toJSON() {
    return {
      message: super.toString(),
      key: this.key
    };
  }
}
class Table {
  constructor(check, pool, name) {
    this.check = check;
    this.pool = pool;
    this.name = name;
    TableName.check(this.name);
    this.tableName = this.name;
    this.logger = logger.child({ table: this.name });
  }
  async init() {
    const { tableName } = this;
    const client = await this.pool.connect();
    const table = [
      `CREATE TABLE "${tableName}" ( data JSONB NOT NULL )`,
      `CREATE UNIQUE INDEX "${tableName} _id" ON "${tableName}" ((data->'_id'))`,
      `CREATE INDEX "${tableName} gin" ON "${tableName}" USING GIN(data jsonb_path_ops)`,
      `CREATE INDEX "${tableName} btree" ON "${tableName}" USING BTREE(data)`
    ];
    try {
      await client.query("BEGIN");
      for (const q of table) {
        await client.query(q);
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      const code = err?.code;
      if (code === "42P07" || code === "23505") {
        this.logger.info({}, "init: duplicate (ignored)");
      } else {
        throw err;
      }
    } finally {
      client.release();
    }
  }
  async put(data) {
    this.check(data);
    const { tableName } = this;
    const client = await this.pool.connect();
    const _rev = revision(data);
    const finalData = this.check({ ...data, _rev });
    if (data._rev) {
      const key = { _id: data._id, _rev: data._rev };
      const res = await client.query(`
        UPDATE "${tableName}"  SET data = $1 WHERE data @> $2
      `, [finalData, key]);
      if (res.rowCount !== 1) {
        throw new TableError("Missing", key);
      }
      client.release();
      return finalData;
    } else {
      const res = await client.query(`
        INSERT INTO "${tableName}"(data) VALUES ($1)
      `, [
        finalData
      ]);
      if (res.rowCount !== 1) {
        throw new TableError("Invalid", data);
      }
      client.release();
      return finalData;
    }
  }
  async get(_id) {
    DocumentId.check(_id);
    const stream = await this.query({ _id });
    const res = await stream.take(1).last();
    if (res) {
      return this.check(res);
    } else {
      return void 0;
    }
  }
  async delete(data) {
    this.check(data);
    Revision.check(data._rev);
    const { tableName } = this;
    const client = await this.pool.connect();
    const key = { _id: data._id, _rev: data._rev };
    const res = await client.query(`
      DELETE FROM "${tableName}" WHERE data @> $1
    `, [key]);
    client.release();
    if (res.rowCount !== 1) {
      throw new TableError(`Missing`, key);
    }
  }
  async query(query) {
    const { tableName } = this;
    const select = typeof query === "string" ? `SELECT data FROM "${tableName}" WHERE data @@ $1` : `SELECT data FROM "${tableName}" WHERE data @> $1`;
    const queryStream = new import_pg_query_stream.default(select, [query]);
    const lake = await buildLake(queryStream, this.pool);
    return lake.map((data) => this.check(data));
  }
}
const buildLake = async (queryStream, pool) => {
  const stream = await new Promise((resolve, reject) => {
    pool.connect((err, client, done) => {
      if (err) {
        reject(err);
        return;
      }
      const stream2 = client.query(queryStream);
      stream2.on("end", done);
      resolve(stream2);
    });
  });
  return (0, import_lake.from)(stream).map(({ data }) => data);
};
