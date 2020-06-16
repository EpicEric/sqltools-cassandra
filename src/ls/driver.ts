import * as CassandraLib from 'cassandra-driver';
import AbstractDriver from '@sqltools/base-driver';
import Queries from './queries';
// import sqltoolsRequire from '@sqltools/base-driver/dist/lib/require';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { v4 as generateId } from 'uuid';

/**
 * MOCKED DB DRIVER
 * THIS IS JUST AN EXAMPLE AND THE LINES BELOW SHOUDL BE REMOVED!
 */
// import fakeDbLib from './mylib'; // this is what you should do
const fakeDbLib = {
  open: () => Promise.resolve(fakeDbLib),
  query: (..._args: any[]) => {
    const nResults = parseInt((Math.random() * 1000).toFixed(0));
    const nCols = parseInt((Math.random() * 100).toFixed(0));
    const colNames = [...new Array(nCols)].map((_, index) => `col${index}`);
    const generateRow = () => {
      const row = {};
      colNames.forEach(c => {
        row[c] = Math.random() * 1000;
      });
      return row;
    }
    const results = [...new Array(nResults)].map(generateRow);
    return Promise.resolve([results]);
  },
  close: () => Promise.resolve(),
};


/* LINES ABOVE CAN BE REMOVED */

interface CQLBatch {
  query: string,
  statements: string[],
  options: CassandraLib.QueryOptions,
}

export default class CqlDriver
  extends AbstractDriver<CassandraLib.Client, CassandraLib.ClientOptions>
  implements IConnectionDriver
{
  queries = Queries.queries;
  isLegacy = false;

  /** if you need to require your lib in runtime and then
   * use `this.lib.methodName()` anywhere and vscode will take care of the dependencies
   * to be installed on a cache folder
   **/
  // private get lib() {
  //   return sqltoolsRequire('node-packge-name') as DriverLib;
  // }

  public async open() {
    if (this.connection) {
      return this.connection;
    }
    const cqlOptions: CassandraLib.ClientOptions = this.credentials.cqlOptions || {};
    const clientOptions: CassandraLib.ClientOptions = {
      contactPoints: [this.credentials.server],
      keyspace: this.credentials.database ? this.credentials.database : undefined,
      authProvider: new CassandraLib.auth.PlainTextAuthProvider(
        this.credentials.username,
        this.credentials.password
      ),
      protocolOptions: {
        port: this.credentials.port,
      },
      socketOptions: {
        connectTimeout: parseInt(`${this.credentials.connectionTimeout || 5}`, 10) * 1_000,
      },
      policies: {
        loadBalancing: new CassandraLib.policies.loadBalancing.RoundRobinPolicy(),
      },
      ...cqlOptions,
    };
    const conn = new CassandraLib.Client(clientOptions);
    await conn.connect();
    this.connection = Promise.resolve(conn);
    // TODO
    // Check for modern schema support
    // const results = await this.query('SELECT keyspace_name FROM system_schema.tables LIMIT 1');
    // if (results[0].error) {
    //   this.log.extend('info')('Remote Cassandra database is in legacy mode');
    //   this.queries = LegacyQueries;
    //   this.isLegacy = true;
    // }
    return this.connection;
  }

  public async close() {
    if (!this.connection) return Promise.resolve();
    const conn = await this.connection;
    await conn.shutdown();
    this.connection = null;
  }

  /**
   * Creates an array of CQL regular statements and batch statements to execute.
   * @param query
   */
  private cqlParse(query: string): (string|CQLBatch)[] {
    const queries = Utils.query.parse(query, 'cql');  // TODO
    const cqlQueries: (string|CQLBatch)[] = [];
    for (let i = 0; i < queries.length; i++) {
      const query = queries[i];
      const found = query.match(
        /^BEGIN\s+(UNLOGGED\s+|COUNTER\s+)?BATCH\s+(?:USING\s+TIMESTAMP\s+(\d+)\s+)?([\s\S]+)$/i
      );

      if (found) {
        const options: CassandraLib.QueryOptions = {};
        if (found[1]) {
          if (found[1].trim().toUpperCase() === 'COUNTER') {
            options.counter = true;
          } else if (found[1].trim().toUpperCase() === 'UNLOGGED') {
            options.logged = false;
          }
        }
        if (found[2]) {
          options.timestamp = parseInt(found[2], 10);
        }
        const batch = {
          query: found[0],
          statements: [found[3]],
          options
        };
        while (true) {
          if (++i == queries.length) {
            throw new Error('Unterminated batch block; include "APPLY BATCH;" at the end');
          }
          const batchQuery = queries[i];
          batch.query += ` ${batchQuery}`;
          if (batchQuery.match(/^APPLY\s+BATCH\s*;?$/i)) {
            cqlQueries.push(batch);
            break;
          } else {
            batch.statements.push(batchQuery);
          }
        }

      } else {
        cqlQueries.push(query);
      }
    }
    return cqlQueries;
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (queries, opt = {}) => {
    const conn = await this.open();
    const parsedQueries = this.cqlParse(queries);  // TODO
    const results: NSDatabase.IResult[] = [];
    for (let i = 0; i < parsedQueries.length; i++) {
      const q = parsedQueries[i];
      let query: string;
      let result: CassandraLib.types.ResultSet;
      try {
        if (typeof q === 'string') {
          query = q;
          result = await conn.execute(q);
        } else {
          query = q.query;
          result = await conn.batch(q.statements, q.options);
        }
        const cols = result.columns ? result.columns.map(column => column.name) : [];
        const queryresult: NSDatabase.IResult = {
          connId: this.getId(),
          cols,
          messages: [{ date: new Date(), message: `Query ok with ${result.rowLength} result(s)`}],
          query,
          results: result.rows || [],
          requestId: opt.requestId,
          resultId: generateId(),
        };
        results.push(queryresult);
      } catch (e) {
        // Return error and previous queries, as they might have modified data
        const queryresult: NSDatabase.IResult = {
          connId: this.getId(),
          cols: [],
          messages: [e.toString()],
          query,
          results: [],
          error: true,
          requestId: opt.requestId,
          resultId: generateId(),
        };
        results.push(queryresult);
        // continue;
        return results;
      }
    }
    return results;
  }

  public async testConnection() {
    await this.open();
    await this.query('SELECT now() FROM system.local', {});
  }

  // TODO
  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        let i = 0;
        return <NSDatabase.IColumn[]>[{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        },{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        },{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        },{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        },{
          database: 'fakedb',
          label: `column${i++}`,
          type: ContextValue.COLUMN,
          dataType: 'faketype',
          schema: 'fakeschema',
          childType: ContextValue.NO_CHILD,
          isNullable: false,
          iconName: 'column',
          table: parent,
        }];
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  // TODO
  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({ parent, item }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    console.log({ item, parent });
    switch (item.childType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        let i = 0;
        return <MConnectionExplorer.IChildItem[]>[{
          database: 'fakedb',
          label: `${item.childType}${i++}`,
          type: item.childType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },{
          database: 'fakedb',
          label: `${item.childType}${i++}`,
          type: item.childType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },
        {
          database: 'fakedb',
          label: `${item.childType}${i++}`,
          type: item.childType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        }];
    }
    return [];
  }

  // TODO
  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(itemType: ContextValue, search: string, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        let j = 0;
        return [{
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },{
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        },
        {
          database: 'fakedb',
          label: `${search || 'table'}${j++}`,
          type: itemType,
          schema: 'fakeschema',
          childType: ContextValue.COLUMN,
        }]
      case ContextValue.COLUMN:
        let i = 0;
        return [
          {
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          },{
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          },{
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          },{
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          },{
            database: 'fakedb',
            label: `${search || 'column'}${i++}`,
            type: ContextValue.COLUMN,
            dataType: 'faketype',
            schema: 'fakeschema',
            childType: ContextValue.NO_CHILD,
            isNullable: false,
            iconName: 'column',
            table: 'fakeTable'
          }
        ];
    }
    return [];
  }

  // TODO
  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return {};
  }
}

/* Copied queries from original codebase */

// public async getTables(): Promise<NSDatabase.ITable[]> {
//   const [queryResults, columnsResults] = await this.query(this.queries.fetchTables);
//   const numberOfColumnsMap: {string: {string: number}} = columnsResults.results
//     .reduce((prev, curr) => prev.concat(curr), [])
//     .reduce((acc: {string: {string: number}}, obj: any) =>
//   {
//     if (typeof acc[obj.keyspace_name] === 'undefined') {
//       acc[obj.keyspace_name] = {};
//     }
//     if (typeof acc[obj.keyspace_name][obj.table_name] === 'undefined') {
//       acc[obj.keyspace_name][obj.table_name] = 1;
//     } else {
//       acc[obj.keyspace_name][obj.table_name] += 1;
//     }
//     return acc;
//   }, {});
//   return queryResults.results.reduce((prev, curr) => prev.concat(curr), []).map((obj: any) => {
//     const table: NSDatabase.ITable = {
//       name: obj.table_name,
//       isView: false,
//       tableSchema: obj.keyspace_name,
//       numberOfColumns: numberOfColumnsMap[obj.keyspace_name] &&
//         numberOfColumnsMap[obj.keyspace_name][obj.table_name],
//       tree: [obj.keyspace_name, 'tables', obj.table_name].join(TREE_SEP)
//     };
//     return table;
//   });
// }

// public async getColumns(): Promise<NSDatabase.IColumn[]> {
//   const [queryResults] = await this.query(this.queries.fetchColumns);
//   return queryResults.results.reduce((prev, curr) => prev.concat(curr), []).map((obj: any) => {
//     const column: NSDatabase.IColumn = {
//       columnName: obj.column_name,
//       tableName: obj.table_name,
//       type: this.isLegacy ? this.mapLegacyTypeToRegularType(obj.type) : obj.type,
//       isNullable: obj.kind === 'regular',
//       isPk: obj.kind !== 'regular',
//       isPartitionKey: obj.kind === 'partition_key',
//       tableSchema: obj.keyspace_name,
//       tree: [obj.keyspace_name, 'tables', obj.table_name, obj.column_name].join(TREE_SEP)
//     };
//     return column;
//   });
// }

// /**
//  * Turns a legacy Cassandra validator into a human-readable type. Examples:
//  * - 'org.apache.cassandra.db.marshal.Int32Type' becomes 'Int32'
//  * - 'org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)'
//  *   becomes 'Set(UTF8)'
//  * @param legacyType
//  */
// private mapLegacyTypeToRegularType(legacyType: string): string {
//   return legacyType.replace(/\b\w+\.|Type\b/g, '');
// }

// public async getFunctions(): Promise<NSDatabase.IFunction[]> {
//   const [queryResults] = await this.query(this.queries.fetchFunctions);
//   return queryResults.results.reduce((prev, curr) => prev.concat(curr), []).map((obj: any) => {
//     const func: NSDatabase.IFunction = {
//       name: obj.function_name,
//       schema: obj.keyspace_name,
//       database: '',
//       signature: obj.argument_types ? `(${obj.argument_types.join(',')})` : '()',
//       args: obj.argument_names,
//       resultType: obj.return_type,
//       source: obj.body,
//       tree: [obj.keyspace_name, 'functions', obj.function_name].join(TREE_SEP),
//     };
//     return func;
//   });
// }

// public async describeTable(prefixedTable: string) {
//   const [keyspace, table] = prefixedTable.split('.');
//   return this.query(Utils.replacer(this.queries.describeTable, {keyspace, table}));
// }

// public async showRecords(prefixedTable: string, limit: number) {
//   const [keyspace, table] = prefixedTable.split('.');
//   return this.query(Utils.replacer(this.queries.fetchRecords, {keyspace, table, limit}));
// }
