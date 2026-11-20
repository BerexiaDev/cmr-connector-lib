import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import oracledb
from loguru import logger

from .sql_connector import SqlConnector, is_retryable_connection_error
from .sql_connector_utils import (
    cast_oracle_to_postgresql_type,
    cast_oracle_to_typescript,
    safe_convert_to_string,
)

# Fetch CLOB/NCLOB as str and BLOB as bytes instead of LOB locators.
oracledb.defaults.fetch_lobs = False
# Fetch NUMBER as Decimal: the default float loses precision on amounts loaded into PostgreSQL NUMERIC.
oracledb.defaults.fetch_decimals = True

# Only plain literals are portable to the PostgreSQL DDL built from extract_table_schema.
_PORTABLE_DEFAULT = re.compile(
    r"(?:NULL|[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|'(?:[^'\\\x00]|'')*')",
    re.IGNORECASE | re.ASCII,
)


class OracleConnector(SqlConnector):
    """
    Oracle source connector (python-oracledb, thin mode: no Instant Client needed).

    ``database`` is the service name (DSN = host:port/service).
    ``schema`` is the owner whose tables are exposed; defaults to the login user.
    Unquoted Oracle identifiers are stored uppercase, while CMR stores reference
    table names lowercase, so table/schema identifiers are normalised to uppercase.
    """

    def __init__(self, host, user, password, port, database, schema=None):
        super().__init__(host, user, password, port, database)
        self.driver = "oracledb"
        self.schema = (schema or user or "").strip().upper()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------
    def _open_connection(self):
        conn = oracledb.connect(
            user=self.user,
            password=self.password,
            dsn=f"{self.host}:{self.port}/{self.database}",
            tcp_connect_timeout=10,
        )
        try:
            conn.outputtypehandler = self._oracle_type_handler
            if self.schema and self.schema != (self.user or "").upper():
                # Unqualified table names (used by the sync service) resolve in this schema.
                with conn.cursor() as cursor:
                    cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self._quote_identifier(self.schema)}")
        except BaseException:
            self._safe_close(conn)
            raise
        return conn

    def get_connection(self):
        return self._connect_with_retry(self._open_connection, "connect")

    def ping(self):
        def _ping(_, cursor):
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.execute("SELECT COUNT(*) FROM all_users WHERE username = :1", [self.schema])
            return cursor.fetchone()[0] > 0

        try:
            found = self._run_operation_with_retry("ping", _ping)
        except Exception as e:
            self._log_failure("ping", e)
            return False

        if not found:
            logger.error(f"Oracle schema '{self.schema}' does not exist, or user '{self.user}' cannot see it.")
            return False

        logger.info("Database connection is active.")
        return True

    @staticmethod
    def _oracle_type_handler(cursor, metadata):
        """Return BFILE columns as 'DIRECTORY/filename' strings."""
        if metadata.type_code == oracledb.DB_TYPE_BFILE:
            def bfile_out_converter(bfile_obj):
                if bfile_obj and hasattr(bfile_obj, "getfilename"):
                    dir_alias, filename = bfile_obj.getfilename()
                    return f"{dir_alias}/{filename}"
                return None

            return cursor.var(metadata.type_code, arraysize=cursor.arraysize, outconverter=bfile_out_converter)
        return None

    # ------------------------------------------------------------------
    # Identifiers
    # ------------------------------------------------------------------
    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        normalized = (identifier or "").strip().strip('"').upper()
        return '"' + normalized.replace('"', '""') + '"'

    def _split_table(self, table_name: str) -> Tuple[str, str]:
        """Accepts 'table' or 'owner.table'; returns uppercase (owner, table)."""
        name = (table_name or "").strip().replace('"', "")
        if "." in name:
            owner, pure_table = name.split(".", 1)
            return owner.strip().upper(), pure_table.strip().upper()
        return self.schema, name.upper()

    def _qualify_table(self, table_name: str) -> str:
        owner, pure_table = self._split_table(table_name)
        return f"{self._quote_identifier(owner)}.{self._quote_identifier(pure_table)}"

    # ------------------------------------------------------------------
    # Catalog
    # ------------------------------------------------------------------
    def get_connection_tables(self):
        sql = """
            SELECT table_name
            FROM all_tables
            WHERE owner = :1
            ORDER BY table_name
        """
        def _get_tables(_, cursor):
            cursor.execute(sql, [self.schema])
            return [row[0] for row in cursor.fetchall()]

        return self._run_operation_with_retry("get_connection_tables", _get_tables)

    def get_connection_columns(self, table_name):
        owner, pure_table = self._split_table(table_name)
        sql = """
            SELECT column_name, data_type
            FROM all_tab_columns
            WHERE owner = :1 AND table_name = :2
            ORDER BY column_id
        """
        try:
            def _get_columns(_, cursor):
                cursor.execute(sql, [owner, pure_table])
                return [
                    {"name": column_name, "type": cast_oracle_to_typescript(data_type)}
                    for column_name, data_type in cursor.fetchall()
                ]

            return self._run_operation_with_retry(f"get_connection_columns:{table_name}", _get_columns)
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []

    def extract_table_schema(self, table_name):
        owner, pure_table = self._split_table(table_name)
        schema_sql = """
            SELECT
                c.column_id AS position,
                c.column_name AS name,
                c.data_type,
                c.data_length AS max_length,
                CASE WHEN c.nullable = 'N' THEN 'NO' ELSE 'YES' END AS is_nullable,
                c.data_default AS default_value,
                CASE WHEN EXISTS (
                    SELECT 1 FROM all_constraints con
                    JOIN all_cons_columns cc ON cc.owner = con.owner AND cc.constraint_name = con.constraint_name
                    WHERE con.owner = c.owner AND con.table_name = c.table_name
                      AND con.constraint_type = 'P' AND cc.column_name = c.column_name
                ) THEN 'YES' ELSE 'NO' END AS is_primary_key,
                CASE WHEN EXISTS (
                    SELECT 1 FROM all_constraints con
                    JOIN all_cons_columns cc ON cc.owner = con.owner AND cc.constraint_name = con.constraint_name
                    WHERE con.owner = c.owner AND con.table_name = c.table_name
                      AND con.constraint_type = 'R' AND cc.column_name = c.column_name
                ) THEN 'YES' ELSE 'NO' END AS is_foreign_key,
                CASE WHEN EXISTS (
                    SELECT 1 FROM all_ind_columns ic
                    WHERE ic.table_owner = c.owner AND ic.table_name = c.table_name
                      AND ic.column_name = c.column_name
                ) THEN 'YES' ELSE 'NO' END AS is_index
            FROM all_tab_columns c
            WHERE c.owner = :schema_name AND c.table_name = :table_name
            ORDER BY c.column_id
        """
        try:
            def _extract_schema(_, cursor):
                cursor.execute(schema_sql, {"schema_name": owner, "table_name": pure_table})
                return [
                    {
                        "position": row[0],
                        "name": row[1],
                        "type": cast_oracle_to_postgresql_type(row[2]),
                        "length": row[3],
                        "nullable": row[4],
                        "default": self._portable_default(row[5]),
                        "primary_key": row[6],
                        "foreign_key": row[7],
                        "is_index": row[8],
                    }
                    for row in cursor.fetchall()
                ]

            return self._run_operation_with_retry(f"extract_table_schema:{table_name}", _extract_schema)
        except Exception as exc:
            logger.error(f"Error extracting schema for {table_name}: {exc}")
            return []

    @staticmethod
    def _portable_default(value) -> Optional[str]:
        """Keep literal defaults only; Oracle expressions (SYSDATE, seq.NEXTVAL, USER...) don't exist in PostgreSQL."""
        if value is None:
            return None
        text = str(value).strip()
        return text if text and _PORTABLE_DEFAULT.fullmatch(text) else None

    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        owner, pure_table = self._split_table(table_name)
        sql = """
            SELECT
                c.index_name,
                c.column_name,
                i.uniqueness,
                CASE WHEN EXISTS (
                    SELECT 1 FROM all_constraints con
                    WHERE con.owner = i.table_owner AND con.table_name = i.table_name
                      AND con.constraint_type = 'P' AND con.index_name = i.index_name
                ) THEN 1 ELSE 0 END AS is_primary
            FROM all_ind_columns c
            JOIN all_indexes i
              ON i.owner = c.index_owner
             AND i.index_name = c.index_name
            WHERE c.table_owner = :schema_name
              AND c.table_name = :table_name
            ORDER BY c.index_name, c.column_position
        """
        try:
            def _get_indexes(_, cursor):
                cursor.execute(sql, {"schema_name": owner, "table_name": pure_table})
                indexes: Dict[str, Dict[str, Any]] = {}
                for idx_name, col_name, uniqueness, is_primary in cursor.fetchall():
                    entry = indexes.setdefault(idx_name, {
                        "name": idx_name,
                        "unique": uniqueness == "UNIQUE" or bool(is_primary),
                        "primary": bool(is_primary),
                        "columns": [],
                    })
                    entry["columns"].append(col_name)
                return [idx for idx in indexes.values() if idx["columns"]]

            return self._run_operation_with_retry(f"get_table_indexes:{owner}.{pure_table}", _get_indexes)
        except Exception as e:
            logger.error(f"Error getting indexes for Oracle table {owner}.{pure_table}: {e}")
            return []

    # ------------------------------------------------------------------
    # Data
    # ------------------------------------------------------------------
    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100) -> List[dict]:
        query = (
            f"SELECT * FROM {self._qualify_table(table_name)} "
            f"OFFSET {int(offset)} ROWS FETCH NEXT {int(limit)} ROWS ONLY"
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

    def fetch_batch(self, cursor, table_name: str, offset: int, limit: int = 100):
        query = (
            f"SELECT * FROM {self._qualify_table(table_name)} "
            f"OFFSET {int(offset)} ROWS FETCH NEXT {int(limit)} ROWS ONLY"
        )
        try:
            def _fetch(_, active_cursor):
                active_cursor.execute(query)
                return active_cursor.fetchall()

            return self._run_operation_with_retry(f"fetch_batch:{table_name}", _fetch)
        except Exception as exc:
            logger.error(f"Error fetching batch from {table_name}: {exc}")
            return []

    def stream_batch(self, cursor, table_name: str, batch_size: int = 10_000, query: str = None, params=None):
        """
        Full-sync streaming: sequentially fetch rows without OFFSET.
        Reconnects only if the failure happens before the first rows are yielded.
        """
        sql = query or f"SELECT * FROM {self._qualify_table(table_name)}"
        bind_params = list(params) if params else []
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
                    if not query_started:
                        active_cursor.arraysize = batch_size
                        active_cursor.prefetchrows = batch_size + 1
                        active_cursor.execute(sql, bind_params)
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
                        raise

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

    def count_table_rows(self, table_name: str, filters=None) -> int:
        try:
            def _count(_, cursor):
                cursor.execute(f"SELECT COUNT(*) FROM {self._qualify_table(table_name)}")
                count_result = cursor.fetchone()
                return int(count_result[0]) if count_result else 0

            return self._run_operation_with_retry(f"count_table_rows:{table_name}", _count)
        except Exception as e:
            logger.error(f"Error getting table total rows: {e}")
            return 0

    def get_min_max_date(self, table_name: str, column_name: str):
        column = self._quote_identifier(column_name)
        sql = (
            f"SELECT MIN({column}), MAX({column}) "
            f"FROM {self._qualify_table(table_name)} "
            f"WHERE {column} IS NOT NULL"
        )
        try:
            def _get_min_max(_, cursor):
                cursor.execute(sql)
                row = cursor.fetchone()
                return (row[0], row[1]) if row else (None, None)

            return self._run_operation_with_retry(f"get_min_max_date:{table_name}", _get_min_max)
        except Exception as e:
            logger.error(f"Error getting min/max for {table_name}.{column_name}: {e}")
            return (None, None)

    def fetch_deltas(
        self,
        cursor,
        primary_keys: List[str],
        log_table: str,
        since_ts: datetime,
        batch_size: int = 10_000,
    ):
        if isinstance(primary_keys, str):
            primary_keys = [primary_keys]
        primary_keys = [pk for pk in (primary_keys or []) if pk]
        if not primary_keys:
            raise ValueError("fetch_deltas requires at least one primary key column.")

        partition_cols = ", ".join(self._quote_identifier(pk) for pk in primary_keys)
        sql = f"""
            SELECT * FROM (
                SELECT t.*,
                       ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY "Date_operation" DESC) AS rn
                FROM {self._qualify_table(log_table)} t
                WHERE "Date_operation" > :1
            )
            WHERE rn = 1
            ORDER BY {partition_cols}
            OFFSET :2 ROWS FETCH NEXT :3 ROWS ONLY
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
                    active_cursor.execute(sql, [since_ts, offset, batch_size])
                    rows = active_cursor.fetchall()
                    if not rows:
                        break

                    col_names = [col[0] for col in active_cursor.description]
                    for row in rows:
                        row_dict = dict(zip(col_names, row))
                        row_dict.pop("RN", None)  # injected ROW_NUMBER column
                        yield row_dict

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

    def truncate_table(self, table_name: str) -> bool:
        try:
            def _truncate(_, cursor):
                cursor.execute(f"TRUNCATE TABLE {self._qualify_table(table_name)}")
                return True

            return self._run_operation_with_retry(f"truncate_table:{table_name}", _truncate)
        except Exception as exc:
            logger.error(f"Error truncating {table_name}: {exc}")
            return False
