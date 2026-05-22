from datetime import datetime
import time
from typing import Any, Dict, List, Tuple

import pyodbc
from loguru import logger

from .sql_connector import SqlConnector, is_retryable_connection_error
from .sql_connector_utils import (
    safe_convert_to_string,
    cast_sqlserver_to_typescript_types,
    cast_sqlserver_to_postgresql_type,
)


class SqlServerConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "ODBC Driver 17 for SQL Server"

    def _open_connection(self):
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "Connection Timeout=10;"
        )
        return pyodbc.connect(conn_str, timeout=10)

    def get_connection(self):
        """Returns a pyodbc connection object directly."""
        return self._connect_with_retry(
            self._open_connection,
            "connect",
        )

    def ping(self):
        return super().ping()

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100) -> List[dict]:
        query = (
            f"SELECT * "
            f"FROM {table_name} "
            f"ORDER BY (SELECT NULL) "
            f"OFFSET {offset} ROWS "
            f"FETCH NEXT {limit} ROWS ONLY;"
        )
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        try:
            def _extract(_, cursor):
                cursor.execute(query)
                cols = [col[0] for col in cursor.description]
                return [
                    {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(cols)}
                    for row in cursor.fetchall()
                ]

            return self._run_operation_with_retry(f"extract_data_batch:{table_name}", _extract)
        except Exception as exc:
            logger.error(f"Error extracting batch from {table_name}: {exc}")
            return []

    def fetch_batch(self, cursor: pyodbc.Cursor, table_name: str, offset: int, limit: int = 100):
        query = (
            f"SELECT * FROM {table_name} "
            f"ORDER BY (SELECT NULL) "
            f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;"
        )
        try:
            def _fetch(_, active_cursor):
                active_cursor.execute(query)
                return active_cursor.fetchall()

            return self._run_operation_with_retry(f"fetch_batch:{table_name}", _fetch)
        except Exception as exc:
            logger.error(f"Error fetching batch from {table_name}: {exc}")
            return []

    def stream_batch(self, cursor: pyodbc.Cursor, table_name: str, batch_size: int = 10_000):
        """
        Full-sync streaming: sequentially fetch rows without OFFSET.
        Works best for reloads (truncate + reload). Not suitable for resume.
        """
        active_connection = None
        active_cursor = None
        owns_retry_resources = False
        rows_emitted = 0
        retry_number = 0
        query_started = False

        try:
            active_connection = self._open_connection()
            active_cursor = active_connection.cursor()
            owns_retry_resources = True

            while True:
                try:
                    active_cursor.arraysize = batch_size
                    if not query_started:
                        active_cursor.execute(f"SELECT * FROM {table_name};")
                        query_started = True

                    rows = active_cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    rows_emitted += len(rows)
                    yield rows
                except Exception as exc:
                    can_retry = (
                        rows_emitted == 0
                        and is_retryable_connection_error(exc)
                        and retry_number < self.max_connection_retries
                    )
                    if not can_retry:
                        logger.error(f"Error streaming batch from {table_name}: {exc}")
                        return

                    self._safe_cleanup(active_cursor, active_connection)
                    retry_number += 1
                    self._log_retry(f"stream_batch:{table_name}", retry_number, exc)
                    time.sleep(self._retry_delay(retry_number))

                    active_connection = self._open_connection()
                    active_cursor = active_connection.cursor()
                    query_started = False
        finally:
            if owns_retry_resources:
                self._safe_cleanup(active_cursor, active_connection)

    def get_connection_tables(self):
        sql = """
                SELECT  t.name
                FROM sys.tables t
                WHERE t.is_ms_shipped = 0
            """
        try:
            def _get_tables(_, cursor):
                cursor.execute(sql)
                return [row.name for row in cursor.fetchall()]

            return self._run_operation_with_retry("get_connection_tables", _get_tables)
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []

    def get_connection_columns(self, table_name):
        try:
            sql = """
                    SELECT column_name, data_type
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name   = ?;
              """

            def _get_columns(_, cursor):
                cursor.execute(sql, table_name)
                rows = cursor.fetchall()
                return [
                    {
                        "name": row.column_name,
                        "type": cast_sqlserver_to_typescript_types(row.data_type),
                    }
                    for row in rows
                ]

            return self._run_operation_with_retry(f"get_connection_columns:{table_name}", _get_columns)
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []

    def count_table_rows(self, table_name: str) -> int:
        try:
            def _count(_, cursor):
                count_result = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                return int(count_result[0]) if count_result else 0

            return self._run_operation_with_retry(f"count_table_rows:{table_name}", _count)
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            return 0

    def get_min_max_date(self, table_name: str, column_name: str):
        """
        Returns (min_value, max_value) for a DATE/DATETIME column in SQL Server.
        """
        sql = f"""
            SELECT
                MIN([{column_name}]) AS min_val,
                MAX([{column_name}]) AS max_val
            FROM [{table_name}]
            WHERE [{column_name}] IS NOT NULL;
        """
        try:
            def _get_min_max(_, cursor):
                cursor.execute(sql)
                row = cursor.fetchone()
                return (row[0], row[1]) if row else (None, None)

            return self._run_operation_with_retry(f"get_min_max_date:{table_name}", _get_min_max)
        except Exception as e:
            logger.error(f"Error getting min/max for {table_name}.{column_name}: {e}")
            return (None, None)

    def extract_table_schema(self, table_name):
        try:
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

            def _extract_schema(_, cursor):
                rows = cursor.execute(schema_sql, table_name, table_name, table_name, table_name).fetchall()
                result = []
                seen = set()
                for row in rows:
                    sql_type = row.data_type.upper()

                    if row.max_length == -1:
                        if sql_type in ("VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "TEXT", "NTEXT"):
                            pg_type = "TEXT"
                        elif sql_type in ("VARBINARY", "IMAGE"):
                            pg_type = "BYTEA"
                        elif sql_type == "XML":
                            pg_type = "XML"
                        else:
                            pg_type = "TEXT"
                    else:
                        pg_type = cast_sqlserver_to_postgresql_type(row.data_type)

                    key = row.name.lower()
                    if key in seen:
                        logger.warning(f"Duplicate column '{row.name}' in table {table_name}, skipping")
                        continue

                    seen.add(key)
                    result.append({
                        "position": row.column_id,
                        "name": row.name,
                        "type": pg_type,
                        "length": row.max_length,
                        "nullable": row.is_nullable,
                        "default": row.default_value,
                        "primary_key": row.is_primary_key,
                        "foreign_key": row.is_foreign_key,
                        "is_index": row.is_indexed,
                    })

                return result

            return self._run_operation_with_retry(f"extract_table_schema:{table_name}", _extract_schema)
        except Exception as exc:
            logger.error(f"Error extracting schema for {table_name}: {exc}")
            return []

    def fetch_deltas(
        self,
        cursor,
        primary_keys: List[str],
        log_table: str,
        since_ts: datetime,
        batch_size: int = 10_000,
    ):
        pk_cols = ", ".join(primary_keys)
        partition_expr = pk_cols
        order_expr = "Date_operation DESC"
        order_by_final = ", ".join(primary_keys)

        sql = f"""
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_expr}
                        ORDER BY {order_expr}
                    ) AS rn
                FROM {log_table}
                WHERE Date_operation > ?
            ) AS ranked
            WHERE rn = 1
            ORDER BY {order_by_final}
            OFFSET ? ROWS
            FETCH NEXT ? ROWS ONLY;
        """

        offset = 0
        active_connection = None
        active_cursor = None
        owns_retry_resources = False
        retry_number = 0

        try:
            active_connection = self._open_connection()
            active_cursor = active_connection.cursor()
            owns_retry_resources = True

            while True:
                try:
                    active_cursor.execute(sql, (since_ts, offset, batch_size))
                    rows = active_cursor.fetchall()
                    if not rows:
                        break

                    col_names = [col[0] for col in active_cursor.description]
                    for row in rows:
                        yield dict(zip(col_names, row))

                    offset += batch_size
                except Exception as exc:
                    if not is_retryable_connection_error(exc) or retry_number >= self.max_connection_retries:
                        raise

                    self._safe_cleanup(active_cursor, active_connection)
                    retry_number += 1
                    self._log_retry(f"fetch_deltas:{log_table}", retry_number, exc)
                    time.sleep(self._retry_delay(retry_number))

                    active_connection = self._open_connection()
                    active_cursor = active_connection.cursor()
        finally:
            if owns_retry_resources:
                self._safe_cleanup(active_cursor, active_connection)

    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Return index definitions for a table from SQL Server catalogs.

        Accepts:
        - "agent" (defaults schema to dbo)
        - "dbo.agent" or "schema.table"

        Output example:
        [
        {"name": "IX_agent_sex_datnaiss", "unique": False, "primary": False, "columns": ["sexagent","datnaiss"]},
        {"name": "PK_agent", "unique": True, "primary": True, "columns": ["numaffil"]}
        ]
        """

        def _split_schema_table(t: str) -> Tuple[str, str]:
            t = (t or "").strip()
            t = t.replace("[", "").replace("]", "")
            if "." in t:
                schema_name, pure_table = t.split(".", 1)
                return schema_name.strip(), pure_table.strip()
            return "dbo", t.strip()

        schema_name, pure_table = _split_schema_table(table_name)

        try:
            sql = """
            SELECT
            i.name AS index_name,
            i.is_unique,
            i.is_primary_key,
            STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns_csv
            FROM sys.indexes i
            JOIN sys.objects o
            ON o.object_id = i.object_id
            JOIN sys.schemas s
            ON s.schema_id = o.schema_id
            JOIN sys.index_columns ic
            ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c
            ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE s.name = ?
            AND o.name = ?
            AND o.type = 'U'
            AND i.name IS NOT NULL
            AND i.is_hypothetical = 0
            AND i.type_desc IN ('CLUSTERED', 'NONCLUSTERED')
            AND ic.is_included_column = 0
            AND ic.key_ordinal > 0
            GROUP BY i.name, i.is_unique, i.is_primary_key
            ORDER BY i.is_primary_key DESC, i.is_unique DESC, i.name;
            """

            def _get_indexes(_, cursor):
                cursor.execute(sql, (schema_name, pure_table))
                rows = cursor.fetchall()

                results: List[Dict[str, Any]] = []
                for row in rows:
                    idxname = row[0]
                    is_unique = bool(row[1])
                    is_primary = bool(row[2])
                    cols_csv = row[3] or ""
                    cols = [col.strip() for col in cols_csv.split(",") if col.strip()]
                    if not cols:
                        continue

                    results.append(
                        {
                            "name": idxname,
                            "unique": bool(is_unique or is_primary),
                            "primary": bool(is_primary),
                            "columns": cols,
                        }
                    )

                return results

            return self._run_operation_with_retry(
                f"get_table_indexes:{schema_name}.{pure_table}",
                _get_indexes,
            )
        except Exception as e:
            logger.error(f"Error getting indexes for SQL Server table {schema_name}.{pure_table}: {e}")
            return []
