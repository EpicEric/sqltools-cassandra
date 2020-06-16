import { IBaseQueries } from '@sqltools/types';
import queryFactory from '@sqltools/base-driver/dist/lib/factory';

// Cassandra >= 3.0
const queries = {
  // TODO
  describeTable: queryFactory`
    SELECT * FROM system_schema.tables
    WHERE keyspace_name = ':keyspace'
    AND table_name = ':table'
    ` as IBaseQueries['describeTable'],
  fetchColumns: queryFactory`
    SELECT keyspace_name, table_name,
    column_name, kind, type
    FROM system_schema.columns
    ` as IBaseQueries['fetchColumns'],
  // TODO
  fetchRecords: queryFactory`
    SELECT * FROM :keyspace.:table LIMIT :limit
    ` as IBaseQueries['fetchRecords'],
  fetchTables: queryFactory`
    SELECT keyspace_name, table_name
    FROM system_schema.tables;
    SELECT keyspace_name, table_name
    FROM system_schema.columns
    ` as IBaseQueries['fetchTables'],
  fetchFunctions: queryFactory`
    SELECT keyspace_name, function_name,
    argument_names, argument_types,
    return_type, body
    FROM system_schema.functions
    ` as IBaseQueries['fetchFunctions'],
  // TODO
  countRecords: null,
  // TODO
  searchTables: null,
  // TODO
  searchColumns: null,
};

// Cassandra < 3.0
const legacyQueries = {
  // TODO
  describeTable: queryFactory`
    SELECT * FROM system.schema_columnfamilies
    WHERE keyspace_name = ':keyspace'
    AND columnfamily_name = ':table'
    ` as IBaseQueries['describeTable'],
  fetchColumns: queryFactory`
    SELECT keyspace_name, columnfamily_name AS table_name,
    column_name, type AS kind, validator AS type
    FROM system.schema_columns
    ` as IBaseQueries['fetchColumns'],
  // TODO
  fetchRecords: queryFactory`
    SELECT * FROM :keyspace.:table LIMIT :limit
    ` as IBaseQueries['fetchRecords'],
  fetchTables: queryFactory`
    SELECT keyspace_name, columnfamily_name AS table_name
    FROM system.schema_columnfamilies;
    SELECT keyspace_name, columnfamily_name AS table_name
    FROM system.schema_columns
    ` as IBaseQueries['fetchTables'],
  fetchFunctions: queryFactory`
    SELECT keyspace_name, function_name,
    argument_names, argument_types,
    return_type, body
    FROM system.schema_functions
    ` as IBaseQueries['fetchFunctions'],
  // TODO
  countRecords: null,
  // TODO
  searchTables: null,
  // TODO
  searchColumns: null,
};

export default {
  queries,
  legacyQueries
};
