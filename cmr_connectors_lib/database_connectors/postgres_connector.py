#!/usr/bin/python
# -*- coding: utf-8 -*-
import re, psycopg2
from datetime import datetime

from loguru import logger
from typing import Dict, Any, List
from pyodbc import Cursor

from .sql_connector import SqlConnector
from cmr_connectors_lib.database_connectors.utils.postgres_connector_utils import _build_select_clause, _build_joins_clause, _build_where_clause, _build_group_by, \
    _build_having_clause
from cmr_connectors_lib.database_connectors.sql_connector_utils import cast_postgres_to_typescript
from .sql_connector_utils import safe_convert_to_string


class PostgresConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, schema):
        super().__init__(host, user, password, port, database)
        self.driver = "postgresql+psycopg2"
        self.schema = schema
    
    def get_connection(self):
        conn_params = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "port": self.port,
            "dbname": self.database,
            "options" : f"-c search_path={self.schema}"
        }

        return psycopg2.connect(**conn_params)


    def extract_data_batch( self, table_name: str, offset: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        query = (
            f"SELECT * FROM {table_name} "
            f"OFFSET {offset} LIMIT {limit};"
        )
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            cols = [c[0] for c in cursor.description]
            return [
                {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(cols)}
                for row in cursor.fetchall()
            ]
        except Exception as exc:
            logger.error(f"Error extracting batch from {table_name}: {exc}")
            return []
        finally:
            cursor.close()
            conn.close()

    def fetch_batch(self, cursor: Cursor, table_name, offset: int, limit: int = 100):
        try:
            query = f'SELECT * FROM {table_name} OFFSET {offset} LIMIT {limit}'
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return []


    def get_connection_tables(self):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type   = 'BASE TABLE';
                """,
                (self.schema,),
            )
            return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def get_connection_columns(self, table_name: str):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                  column_name,
                  data_type,
                  udt_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name   = %s
                ORDER BY ordinal_position;
                """,
                (self.schema, table_name),
            )
            rows = cur.fetchall()

            columns: list[dict[str, str]] = []
            for column_name, data_type, udt_name in rows:
                ts_type = cast_postgres_to_typescript(data_type)
                columns.append({"name": column_name, "type": ts_type})
            return columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
        finally:
            cur.close()
            conn.close()


    def count_table_rows(self, table_name: str) -> int:
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count_result = cursor.fetchone()
            total_count = int(count_result[0]) if count_result else 0
            return total_count
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            return 0
        finally:
            cursor.close()
            connection.close()

    def extract_table_schema(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            schema_sql = """
                      SELECT
                        a.attnum AS position,
                        a.attname AS name,
                        format_type(a.atttypid, a.atttypmod) AS data_type,
                        a.attlen AS max_length,
                        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
                        COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '') AS default_value,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM pg_index i
                                WHERE i.indrelid = c.oid
                                  AND i.indisprimary
                                  AND a.attnum = ANY(i.indkey)
                            )
                            THEN 'YES'
                            ELSE 'NO'
                        END AS is_primary_key,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM pg_constraint fk
                                WHERE fk.contype = 'f'
                                  AND fk.conrelid = c.oid
                                  AND a.attnum = ANY(fk.conkey)
                            )
                            THEN 'YES'
                            ELSE 'NO'
                        END AS is_foreign_key,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM pg_index i2
                                WHERE i2.indrelid = c.oid
                                  AND NOT i2.indisprimary
                                  AND a.attnum = ANY(i2.indkey)
                            )
                            THEN 'YES'
                            ELSE 'NO'
                        END AS is_index
                    FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_type t ON a.atttypid = t.oid
                    LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = %s
                      AND c.relname = %s
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum;
                """

            cursor.execute(schema_sql, (self.schema, table_name))
            rows = cursor.fetchall()

            result = [
                {
                    "position": row[0],
                    "name": row[1],
                    "type": row[2].upper(),
                    "length": row[3],
                    "nullable": row[4],
                    "default": row[5],
                    "primary_key": row[6],
                    "foreign_key": row[7],
                    "is_index": row[8],
                }
                for row in rows
            ]

            return result

        except Exception as exc:
            logger.error(f"Error extracting schema for {table_name}: {exc}")
            return []
        finally:
            cursor.close()
            conn.close()


    def create_schema_if_missing(self, schema_name: str):
        """Creates a schema in PostgreSQL if it doesn't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
            conn.commit()
            logger.info(f"Schema {schema_name} created or already exists.")
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

            
    def create_table_if_missing(self, table_name:str, create_table_statement: str, index_table_statement:str = None):
        """Creates a table in PostgreSQL if it doesn't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(create_table_statement)
            if index_table_statement:
                cursor.execute(index_table_statement)
            conn.commit()
            logger.info(f"Table {table_name} created or already exists.")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()


    
    def  build_create_table_statement(self, table_name: str, schema_name: str = 'public', columns = []):
        """
        Generates a PostgreSQL CREATE TABLE statement along with a CREATE INDEX statement
        (for indexed columns) using the provided column metadata.
        """
        column_defs = []
        primary_keys = []
        index_keys = []
        # matches func_name(…)  or   schema.func_name(…)
        fun_call = re.compile(r'^[A-Za-z_][\w\.]*\s*\(.*\)$')
        for col in columns:
            col_name = col["name"]
            col_type = col["type"].upper()
            length = col.get("length")
            nullable = col["nullable"].strip() == "YES"
            default = col["default"] or ""
            is_pk = col["primary_key"].strip() == "YES"
            if col['is_index'] == "YES":
                index_keys.append(col_name)

            # Handle types with length
            if col_type in ("VARCHAR", "CHAR") and length:
                col_type_str = f"{col_type}({length})"
            else:
                col_type_str = col_type

            # Build column definition
            col_def_parts = [f'"{col_name}"', col_type_str]

            if not nullable:
                col_def_parts.append("NOT NULL")

                # DEFAULT handling
            if default:
                d = default
                # is this a function call?  (unquoted identifier + '(')
                if not fun_call.match(d):
                    # it’s either a literal ('…'), numeric (1234), casted literal ('…'::text), etc.
                    col_def_parts.append(f"DEFAULT {d}")
                # else: skip it

            column_defs.append(" ".join(col_def_parts))

            if is_pk:
                primary_keys.append(f'"{col_name}"')

        # Append primary key constraint
        if primary_keys:
            pk_def = f"PRIMARY KEY ({', '.join(primary_keys)})"
            column_defs.append(pk_def)

        columns_sql = ",\n  ".join(column_defs)
        create_stmt = f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (\n  {columns_sql}\n);'
        index_stmt = None
        if index_keys:
            index_name = f"idx_{schema_name}_{table_name}_{'_'.join(index_keys)}"
            cols_sql = ", ".join(index_keys)
            index_stmt = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_name}.{table_name} ({cols_sql});"

        return create_stmt, index_stmt


    def get_view_columns(self, table_name: str, schema_name: str = 'populations'):
        """
           Returns a list of dicts with column names and mapped TypeScript types
           for the given Postgres view in the given schema.

           Args:
               table_name: The name of the table to get columns for
               schema: Schema name, defaults to "populations"
        """
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                    SELECT
                        a.attname AS column_name,
                        trim( split_part( pg_catalog.format_type(a.atttypid, a.atttypmod)
                                        , '(' , 1) ) AS data_type
                    FROM   pg_attribute   AS a
                    JOIN   pg_class       AS c ON c.oid          = a.attrelid
                    JOIN   pg_namespace   AS n ON n.oid          = c.relnamespace
                    WHERE  c.relkind   = 'm'
                      AND  n.nspname   = %s
                      AND  c.relname   = %s
                      AND  a.attnum    > 0
                      AND  NOT a.attisdropped
                    ORDER  BY a.attnum;
                  """,
                (schema_name, table_name),
            )
            rows = cur.fetchall()

            columns: list[dict[str, str]] = []
            for column_name, data_type in rows:
                ts_type = cast_postgres_to_typescript(data_type)
                columns.append({"name": column_name, "type": ts_type})
            return columns
        except Exception as e:
            logger.error(f"Error getting view columns: {e}")
            return []

        finally:
            cur.close()
            conn.close()

    def fetch_deltas(self, cursor, primary_key: str, log_table: str, since_ts: datetime, batch_size: int = 10_000):
        sql = f"""
            SELECT DISTINCT ON ({primary_key}) *
            FROM {log_table}
            WHERE op_timestamp > %s
            ORDER BY {primary_key}, 
            op_timestamp DESC
            LIMIT %s OFFSET %s;
        """
        offset = 0
        while True:
            cursor.execute(sql, (since_ts, batch_size, offset))
            rows = cursor.fetchall()
            if not rows:
                break

            col_names = [desc[0] for desc in cursor.description]
            for row in rows:
                yield dict(zip(col_names, row))

            offset += batch_size


    def build_query(self, data: Dict[str, Any], invert_where: bool = False):
        """
        Build an Informix SQL query based on the provided JSON definition.
        """
        try:
            # Step 1: Validate input data
            base_table = data.get('baseTable')
            if not base_table:
                logger.error("Base table is required")
                return None

            # Step 4: Build the SELECT clause
            select_clause = _build_select_clause(data.get('selectedFields', []))

            # Step 5: Build the FROM
            from_clause = f"FROM {base_table}"
            joins_clause = _build_joins_clause(base_table, data.get('joins', []))

            # Step 6: Build the WHERE clause
            where_clause = _build_where_clause(data.get('whereConditions', []), invert_where)

            # Step 7: Build the GROUP BY clause
            group_by_clause = _build_group_by(data.get('groupByFields', []))
            having_clause = _build_having_clause(data.get('having', []))

            # Combine clauses into a list
            clauses = [
                select_clause,
                from_clause,
                joins_clause,
                where_clause,
                group_by_clause,
                having_clause
            ]

            # Join the clauses with a newline if the clause is not empty.
            query = "\n".join(clause for clause in clauses if clause.strip())
            return query

        except Exception as e:
            logger.error(f"Error building query: {str(e)}")
            return None
        
        
    def truncate_table(self, table_name: str, schema: str = None) -> bool:
        """
        Remove all data from the specified table while keeping its structure
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            use_schema = self.schema if self.schema else schema
            truncate_sql = f'TRUNCATE TABLE "{use_schema}"."{table_name}"'
            cursor.execute(truncate_sql)
            conn.commit()
            logger.info(f"Successfully truncated table: {use_schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to truncate table {use_schema}.{table_name}: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()