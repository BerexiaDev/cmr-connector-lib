#!/usr/bin/python
# -*- coding: utf-8 -*-
from typing import List

import pyodbc
from loguru import logger
from .sql_connector import SqlConnector
from .sql_connector_utils import cast_informix_to_typescript_types, cast_informix_to_postgresql_type, \
    safe_convert_to_string, cast_sqlserver_to_typescript_types, cast_sqlserver_to_postgresql_type


class SqlServerConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "ODBC Driver 17 for SQL Server"

    def get_connection(self):
        """Returns a pyodbc connection object directly."""
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
        )
        return pyodbc.connect(conn_str)


    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()  # Ensure the query runs
            logger.info("Database connection is active.")
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False


    def extract_data_batch(
            self, table_name: str, offset: int = 0, limit: int = 100
    ) -> List[dict]:
        """
        Pull `limit` rows starting at `offset` from `table_name`.
        Uses OFFSET/FETCH, which requires an ORDER BY – we fake one with
        `(SELECT NULL)` so the call works for any table.
        """
        query = (
            f"SELECT * "
            f"FROM {table_name} "
            f"ORDER BY (SELECT NULL) "
            f"OFFSET {offset} ROWS "
            f"FETCH NEXT {limit} ROWS ONLY;"
        )
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        try:
            conn = self.get_connection()
            cur = conn.execute(query)
            cols = [c[0] for c in cur.description]
            return [
                {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(cols)}
                for row in cur.fetchall()
            ]
        except Exception as exc:
            logger.error(f"Error extracting batch from {table_name}: {exc}")
            return []


    def fetch_batch(
            self,
            cursor: pyodbc.Cursor,
            table_name: str,
            offset: int,
            limit: int = 100,
    ):
        """
            Fetch up to `limit` rows from `table`, skipping the first `offset` rows.

            Args:
                table_name (str):       Name of the Informix table.
                offset (int):      Number of rows to skip.
                limit (int):       Maximum rows to return.
                cursor:            An SqlServer cursor.

            Returns:
                list of tuple:     The fetched rows, empty if none remain.
        """
        try:
            query = (
                f"SELECT * FROM {table_name} "
                f"ORDER BY (SELECT NULL) "
                f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;"
            )
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as exc:
            logger.error(f"Error fetching batch from {table_name}: {exc}")
            return None

    def get_connection_tables(self) -> List[str]:
        """
        Return user tables (excludes system tables & views) from INFORMATION_SCHEMA.
        """
        sql = """
                SELECT  t.name
                FROM sys.tables t
                WHERE t.is_ms_shipped = 0
            """
        cursor = self.get_connection().cursor()
        cursor.execute(sql)
        tables = [row.name for row in cursor.fetchall()]
        cursor.close()
        return tables

    def get_connection_columns(self, table_name):
        """Returns a list of dictionaries with column names and types for the given table."""
        cursor = self.get_connection().cursor()
        sql = """
                SELECT column_name, data_type
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name   = ?;
          """
        cursor.execute(sql, table_name)
        rows = cursor.fetchall()

        columns = [{'name': row.column_name, 'type': cast_sqlserver_to_typescript_types(row.data_type)} for row in rows]

        cursor.close()
        return columns


    def count_table_rows(self, table_name: str) -> int:
        try:
            connection = self.get_connection()
            count_result = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            total_count = int(count_result[0]) if count_result else 0
            return total_count
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            raise ValueError(f"Failed to get table total rows: {str(e)}")


    def extract_table_schema(self, table_name):
        """
            Gather column-level details from SQL Server including:
            - column name, type, nullability, default
            - primary key, foreign key, and index flags
            """
        try:
            with self.get_connection() as conn:
                schema_sql = """
                        WITH pk_cols AS (
                            SELECT c.name AS col_name
                            FROM sys.indexes i
                            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                            WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1
                        ),
                        fk_cols AS (
                            SELECT c.name AS col_name
                            FROM sys.foreign_key_columns fkc
                            JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
                            WHERE fkc.parent_object_id = OBJECT_ID(?)
                        ),
                        idx_cols AS (
                            SELECT DISTINCT c.name AS col_name
                            FROM sys.indexes i
                            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                            WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 0
                        )
                        SELECT
                            col.column_id,
                            col.name,
                            TYPE_NAME(col.user_type_id) AS data_type,
                            col.max_length,
                            IIF(col.is_nullable = 1, 'YES', 'NO') AS is_nullable,
                            OBJECT_DEFINITION(col.default_object_id) AS default_value,
                            IIF(pk.col_name IS NOT NULL, 'YES', 'NO') AS is_primary_key,
                            IIF(fk.col_name IS NOT NULL, 'YES', 'NO') AS is_foreign_key,
                            IIF(ix.col_name IS NOT NULL, 'YES', 'NO') AS is_indexed
                        FROM sys.columns col
                        LEFT JOIN pk_cols pk ON col.name = pk.col_name
                        LEFT JOIN fk_cols fk ON col.name = fk.col_name
                        LEFT JOIN idx_cols ix ON col.name = ix.col_name
                        WHERE col.object_id = OBJECT_ID(?)
                        ORDER BY col.column_id;
                    """

                result = conn.execute(schema_sql, table_name, table_name, table_name, table_name).fetchall()

                return [
                    {
                        "position": row.column_id,
                        "name": row.name,
                        "type": cast_sqlserver_to_postgresql_type(row.data_type),
                        "length": row.max_length,
                        "nullable": row.is_nullable,
                        "primary_key": row.is_primary_key,
                        "foreign_key": row.is_foreign_key,
                        "is_index": row.is_indexed,
                        "default": (row.default_value or "").strip(),
                    }
                    for row in result
                ]

        except Exception as exc:
            logger.error(f"Error extracting schema for {table_name}: {exc}")
            return []