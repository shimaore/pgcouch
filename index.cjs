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
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var pgcouch_exports = {};
__export(pgcouch_exports, {
  DocumentId: () => DocumentId,
  Revision: () => Revision,
  Table: () => Table,
  TableConflictOnUpdateError: () => TableConflictOnUpdateError,
  TableData: () => TableData,
  TableError: () => TableError,
  TableMissingOnDeleteError: () => TableMissingOnDeleteError,
  TableMissingOnUpdateError: () => TableMissingOnUpdateError,
  TableName: () => TableName,
  buildLake: () => buildLake
});
module.exports = __toCommonJS(pgcouch_exports);
var import_pg_query_stream = __toESM(require("pg-query-stream"), 1);
var import_lake = require("@shimaore/lake");
var import_runtypes = require("runtypes");
var rt = __toESM(require("runtypes"), 1);
var import_crypto = require("crypto");
const TableName = rt.String;
const DocumentId = rt.String.withConstraint((s) => s.length > 0 || "document id must not be the empty string");
const Revision = rt.String.withBrand("Revision").withConstraint((s) => s.length > 0 || "revision must not be the empty string");
const TableData = rt.Record({
  _id: DocumentId,
  _rev: Revision.optional()
});
const revision = (0, import_runtypes.Contract)(
  // Parameters
  TableData,
  // Return type
  Revision
).enforce((a) => {
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
class TableMissingOnUpdateError extends TableError {
}
class TableConflictOnUpdateError extends TableError {
}
class TableMissingOnDeleteError extends TableError {
}
class Table {
  constructor(check, pool, name) {
    this.check = check;
    this.pool = pool;
    this.name = name;
    TableName.check(this.name);
    this.tableName = this.name;
  }
  /** init — Connection and idempotent table creation
   * Must be called before any other operation.
   *
   * @returns true if the table existed
   */
  async init() {
    const { tableName } = this;
    const client = await this.pool.connect();
    const table = [
      `CREATE TABLE "${tableName}" ( data JSONB NOT NULL )`,
      // We store only one record per _id (we do not keep historical revisions,
      // since we are not planning to support master-master replication).
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
      return false;
    } catch (err) {
      await client.query("ROLLBACK");
      const code = err?.code;
      if (code === "42P07" || code === "23505") {
        return true;
      } else {
        return Promise.reject(err);
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
      client.release();
      if (res.rowCount !== 1) {
        return Promise.reject(new TableMissingOnUpdateError("Missing", key));
      }
      return finalData;
    } else {
      const res = await client.query(`
        INSERT INTO "${tableName}"(data) VALUES ($1)
      `, [
        finalData
      ]);
      client.release();
      if (res.rowCount !== 1) {
        return Promise.reject(new TableConflictOnUpdateError("Invalid", data));
      }
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
      return Promise.reject(new TableMissingOnDeleteError(`Missing`, key));
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
      if (!client) {
        reject(new Error("No client"));
        return;
      }
      const stream2 = client.query(queryStream);
      stream2.on("end", done);
      resolve(stream2);
    });
  });
  return (0, import_lake.from)(stream).map(({ data }) => data);
};
