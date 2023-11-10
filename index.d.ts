import { Pool } from 'pg';
import QueryStream from 'pg-query-stream';
import { LakeAsyncIterator } from '@shimaore/lake';
import * as rt from 'runtypes';
/** TableName — ensures the name is a valid (e.g. CouchDB) name.
 */
export declare const TableName: rt.String;
export type TableName = rt.Static<typeof TableName>;
/** TableData — contains at least a field named `_id`
 */
export declare const DocumentId: rt.Constraint<rt.String, string, unknown>;
export type DocumentId = rt.Static<typeof DocumentId>;
export declare const Revision: rt.Constraint<rt.Brand<"Revision", rt.String>, string & rt.RuntypeBrand<"Revision">, unknown>;
export type Revision = rt.Static<typeof Revision>;
export declare const TableData: rt.Record<{
    _id: rt.Constraint<rt.String, string, unknown>;
    _rev: rt.Optional<rt.Constraint<rt.Brand<"Revision", rt.String>, string & rt.RuntypeBrand<"Revision">, unknown>>;
}, false>;
export type TableData = rt.Static<typeof TableData>;
export declare class TableError extends Error {
    private readonly key;
    constructor(msg: string, key: object);
    toJSON(): {
        message: string;
        key: object;
    };
}
export declare class TableMissingOnUpdateError extends TableError {
}
export declare class TableConflictOnUpdateError extends TableError {
}
export declare class TableMissingOnDeleteError extends TableError {
}
export declare class Table<T extends TableData> {
    readonly check: (x: any) => T;
    protected readonly pool: Pool;
    readonly name: TableName;
    protected readonly tableName: string;
    constructor(check: (x: any) => T, pool: Pool, name: TableName);
    /** init — Connection and idempotent table creation
     * Must be called before any other operation.
     *
     * @returns true if the table existed
     */
    init(): Promise<boolean>;
    put(data: T): Promise<T>;
    get(_id: DocumentId): Promise<T | undefined>;
    delete(data: TableData): Promise<void>;
    /**
     * `query` is a generic table query.
     * It can use:
     * - MongoDB-style query = { _id: 'user:bob' }, see
     *   [Containment](https://www.postgresql.org/docs/current/datatype-json.html#JSON-CONTAINMENT)
     * - JSONPatch query = '$.year > 1989', see
     *   [JSON Path](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH)
     * It return an AsyncIterable with extended capabilities.
     */
    query(query: string | object): Promise<LakeAsyncIterator<T>>;
}
export declare const buildLake: (queryStream: QueryStream, pool: Pool) => Promise<LakeAsyncIterator<unknown>>;
//# sourceMappingURL=index.d.ts.map