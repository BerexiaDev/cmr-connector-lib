#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
import time
from typing import Any, List, Dict

import pyodbc
from loguru import logger
from pyodbc import Cursor

from .sql_connector import SqlConnector, is_retryable_connection_error
from .sql_connector_utils import (
    cast_informix_to_typescript_types,
    cast_informix_to_postgresql_type,
    safe_convert_to_string,
)


class InformixConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, protocol, locale):
        super().__init__(host, user, password, port, database)
        self.protocol = protocol
        self.locale = locale
        self.driver_path = "app/main/drivers/ddifcl28.so"

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            if "first" not in query:
                query = query.replace(";", " ")
                query = f"SELECT FIRST {rows} * FROM ({query})"
        return query

    def _open_connection(self):
        conn_str = (
            f"DRIVER={self.driver_path};"
            f"DATABASE={self.database};"
            f"HOSTNAME={self.host};"
            f"PORT={self.port};"
            f"PROTOCOL={self.protocol};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"CLIENT_LOCALE={self.locale};"
            f"DB_LOCALE={self.locale};"
            "LoginTimeout=10;"
        )
        conn = pyodbc.connect(conn_str, timeout=10)
        conn.setdecoding(pyodbc.SQL_CHAR, encoding="latin1")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        return conn

    def get_connection(self):
        return self._connect_with_retry(self._open_connection, "connect")

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100):
        query = f"SELECT SKIP {offset} FIRST {limit} * FROM {table_name};"
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        try:
            def _extract(_, cursor):
                cursor.execute(query)
                rows = cursor.fetchall()
                column_names = [col[0] for col in cursor.description]
                return [
                    {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(column_names)}
                    for row in rows
                ]

            return self._run_operation_with_retry(f"extract_data_batch:{table_name}", _extract)
        except Exception as e:
            logger.error(f"Error extracting batch from {table_name}: {str(e)}")
            return []

    def fetch_batch(self, cursor: Cursor, table_name, offset: int, limit: int = 100):
        query = f"SELECT SKIP {offset} FIRST {limit} * FROM {table_name}"
        try:
            def _fetch(_, active_cursor):
                active_cursor.execute(query)
                return active_cursor.fetchall()

            return self._run_operation_with_retry(f"fetch_batch:{table_name}", _fetch)
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return []

    def stream_batch(self, cursor: pyodbc.Cursor, table_name: str, batch_size: int = 10_000):
        """
        Full-sync streaming for Informix (no SKIP / OFFSET).
        Uses a single SELECT and fetchmany to avoid performance degradation.
        Suitable for truncate + reload scenarios.
        """
        active_connection = None
        active_cursor = None
        owns_retry_resources = False
        rows_emitted = 0
        retry_number = 0
        query_started = False

        try:
            logger.info(f"Start streaming Informix table {table_name} with batch_size={batch_size}")
            active_connection = self._open_connection()
            active_cursor = active_connection.cursor()
            owns_retry_resources = True

            while True:
                try:
                    active_cursor.arraysize = batch_size
                    if not query_started:
                        active_cursor.execute(f"SELECT * FROM {table_name}")
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
                        logger.error(f"Error streaming batch from Informix table {table_name}: {exc}")
                        return

                    self._safe_cleanup(active_cursor, active_connection)
                    retry_number += 1
                    self._log_retry(f"stream_batch:{table_name}", retry_number, exc)
                    time.sleep(self._retry_delay(retry_number))

                    active_connection = self._open_connection()
                    active_cursor = active_connection.cursor()
                    query_started = False

            logger.info(f"Finished streaming Informix table {table_name}")
        finally:
            if owns_retry_resources:
                self._safe_cleanup(active_cursor, active_connection)

    def get_connection_tables(self):
        sql = "SELECT tabname FROM systables WHERE tabtype = 'T' AND tabname NOT LIKE 'sys%'"
        try:
            def _get_tables(_, cursor):
                cursor.execute(sql)
                return [row.tabname for row in cursor.fetchall()]

            return self._run_operation_with_retry("get_connection_tables", _get_tables)
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []

    def get_connection_columns(self, table_name):
        sql = f"""
            SELECT colname, coltype 
            FROM syscolumns 
            WHERE tabid = (SELECT tabid FROM systables WHERE tabname = '{table_name}')
        """
        try:
            def _get_columns(_, cursor):
                cursor.execute(sql)
                rows = cursor.fetchall()
                return [
                    {"name": row.colname, "type": cast_informix_to_typescript_types(row.coltype)}
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

    def get_database_schema(self) -> Dict[str, Dict]:
        try:
            logger.debug("Getting database schema for Informix using pyodbc")

            def _get_schema(_, cursor):
                tables = {}

                try:
                    cursor.execute("SELECT TRIM(DBINFO('dbname')) FROM systables WHERE tabid = 1")
                    current_db = cursor.fetchone()[0]
                    logger.debug(f"Current database: {current_db}")
                except Exception as db_err:
                    if is_retryable_connection_error(db_err):
                        raise
                    logger.warning(f"Could not get current database: {db_err}")

                table_query = """
                    SELECT DISTINCT
                        t.tabname,
                        t.owner
                    FROM systables t
                    WHERE t.tabtype = 'T'
                    AND t.tabid >= 100
                """
                logger.debug(f"Executing table query: {table_query}")
                cursor.execute(table_query)
                table_rows = cursor.fetchall()

                logger.debug(f"Found {len(table_rows)} tables")

                for table_row in table_rows:
                    table_name = table_row[0].strip()
                    owner = table_row[1].strip() if table_row[1] else None

                    logger.debug(f"Processing table: {table_name}, owner: {owner}")

                    column_query = f"""
                        SELECT 
                            c.colname,
                            c.coltype,
                            c.collength
                        FROM syscolumns c
                        JOIN systables t ON c.tabid = t.tabid
                        WHERE t.tabname = '{table_name}'
                        ORDER BY c.colno
                    """

                    try:
                        logger.debug(f"Getting columns for table {table_name}")
                        cursor.execute(column_query)
                        columns = cursor.fetchall()

                        if columns:
                            column_list = []
                            for col in columns:
                                col_name = col[0].strip()
                                col_type = cast_informix_to_typescript_types(col[1])
                                logger.debug(f"Column {col_name} has type {col[1]} wihch is {col_type}")
                                column_list.append(
                                    {
                                        "name": col_name,
                                        "type": col_type,
                                        "length": col[2],
                                    }
                                )

                            tables[table_name] = {
                                "name": table_name,
                                "owner": owner,
                                "columns": column_list,
                            }
                            logger.debug(f"Added table {table_name} with {len(columns)} columns")
                    except Exception as col_err:
                        if is_retryable_connection_error(col_err):
                            raise
                        logger.error(f"Error getting columns for table {table_name}: {col_err}")
                        continue

                logger.debug(f"Retrieved schema for {len(tables)} tables")
                if len(tables) == 0:
                    logger.warning("No tables found in the schema")

                return tables

            return self._run_operation_with_retry("get_database_schema", _get_schema)
        except Exception as e:
            logger.error(f"Error getting database schema: {str(e)}")
            raise ValueError(f"Failed to retrieve database schema: {str(e)}")

    def extract_table_schema(self, table_name):
        query = f"""
            SELECT
            c.colno      AS ordinal_position,
            c.colname,
            c.coltype,
            c.collength,
            CASE
                WHEN BITAND(c.coltype, 256) = 256 THEN 'NO'
                ELSE 'YES'
            END                                       AS is_nullable,

            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM   sysconstraints sc
                    JOIN   sysindexes     si ON sc.idxname = si.idxname
                    WHERE  sc.constrtype = 'P'
                      AND  sc.tabid     = c.tabid
                      AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                            OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                            OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                            OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                            OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                            OR c.colno = si.part16)
                ) THEN 'YES' ELSE 'NO'
            END                                       AS is_primary_key,

            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM   sysconstraints sc
                    JOIN   sysreferences  sr ON sc.constrid = sr.constrid
                    JOIN   sysindexes     si ON sc.idxname  = si.idxname
                    WHERE  sc.constrtype = 'R'
                      AND  sc.tabid      = c.tabid
                      AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                            OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                            OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                            OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                            OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                            OR c.colno = si.part16)
                ) THEN 'YES' ELSE 'NO'
            END                                       AS is_foreign_key,


            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM   sysindexes si
                    WHERE  si.tabid = c.tabid
                      AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                            OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                            OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                            OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                            OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                            OR c.colno = si.part16)
                ) THEN 'YES' ELSE 'NO'
            END                                       AS is_index,


            d.default                                 AS default_value
        FROM   syscolumns   c
        JOIN   systables    t ON c.tabid = t.tabid
        LEFT   JOIN sysdefaults d ON c.tabid = d.tabid AND c.colno = d.colno
        WHERE  t.tabname = '{table_name}'
        ORDER  BY c.colno;
        """
        try:
            def _extract_schema(_, cursor):
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {
                        "position": col[0],
                        "name": col[1],
                        "type": cast_informix_to_postgresql_type(col[2]),
                        "length": col[3],
                        "nullable": col[4],
                        "primary_key": col[5],
                        "foreign_key": col[6],
                        "is_index": col[7],
                        "default": col[8],
                    }
                    for col in rows
                ]

            return self._run_operation_with_retry(f"extract_table_schema:{table_name}", _extract_schema)
        except Exception as e:
            logger.error(f"Error getting database schema: {str(e)}")
            return []

    def fetch_deltas(
        self,
        cursor,
        primary_keys: List[str],
        log_table: str,
        since_ts: datetime,
        batch_size: int = 10_000,
    ):
        pk_match = " AND ".join(f"lt2.{pk} = lt1.{pk}" for pk in primary_keys)
        order_by = ", ".join(primary_keys)

        sql = f"""
            SELECT SKIP ? FIRST ? *
            FROM {log_table} lt1
            WHERE lt1.Date_operation = (
                SELECT MAX(lt2.Date_operation)
                FROM {log_table} lt2
                WHERE {pk_match}
                AND lt2.Date_operation > ?
            )
            AND lt1.Date_operation > ?
            ORDER BY {order_by};
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
                    active_cursor.execute(sql, (offset, batch_size, since_ts, since_ts))
                    rows = active_cursor.fetchall()
                    if not rows:
                        break

                    col_names = [col[0] for col in active_cursor.description]
                    for tup in rows:
                        yield dict(zip(col_names, tup))

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

    def get_min_max_date(self, table_name: str, column_name: str):
        """
        Returns (min_value, max_value) for a DATE / DATETIME column in an Informix table.
        Assumes snake_case identifiers (no spaces), so no quoting.
        """
        sql = f"""
            SELECT MIN({column_name}) AS min_val, MAX({column_name}) AS max_val
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
        """
        try:
            def _get_min_max(_, cursor):
                logger.info(f"Getting min/max for {table_name}.{column_name}")
                cursor.execute(sql)
                row = cursor.fetchone()
                return (row[0], row[1]) if row else (None, None)

            return self._run_operation_with_retry(f"get_min_max_date:{table_name}", _get_min_max)
        except Exception as e:
            logger.error(f"Error getting min/max for {table_name}.{column_name}: {e}")
            return (None, None)

    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        try:
            raw = table_name or ""
            lookup = raw.strip()
            lookup_no_quotes = lookup.replace('"', "").replace("'", "").strip()
            if "." in lookup_no_quotes:
                lookup_no_quotes = lookup_no_quotes.split(".")[-1].strip()

            logger.info(f"[IFX][IDX] get_table_indexes raw_table_name={raw!r} lookup={lookup_no_quotes!r}")

            def _get_indexes(_, cursor):
                safe_name = raw.replace("'", "''")
                sql_tabid = (
                    "SELECT tabid FROM systables WHERE tabtype = 'T' AND tabname = '%s'"
                    % safe_name
                )
                logger.info(f"[IFX][IDX] tabid_sql={sql_tabid}")
                cursor.execute(sql_tabid)
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"Table not found in Informix catalogs: {table_name}")
                    return []

                tabid = int(row[0])
                logger.info(f"[IFX][IDX] tabid={tabid} for table={table_name!r}")

                sql_idx = """
                    SELECT
                        idxname, idxtype,
                        part1, part2, part3, part4, part5, part6, part7, part8,
                        part9, part10, part11, part12, part13, part14, part15, part16
                    FROM sysindexes
                    WHERE tabid = ?
                """
                logger.info(f"[IFX][IDX] fetching sysindexes for tabid={tabid}")
                cursor.execute(sql_idx, (tabid,))
                index_rows = cursor.fetchall()
                logger.info(f"[IFX][IDX] sysindexes rows={len(index_rows)} for tabid={tabid}")
                if not index_rows:
                    return []

                cursor.execute("SELECT colno, colname FROM syscolumns WHERE tabid = ?", (tabid,))
                col_map = {int(r[0]): r[1].strip() for r in cursor.fetchall()}
                logger.info(f"[IFX][IDX] syscolumns mapped cols={len(col_map)}")

                cursor.execute(
                    """
                    SELECT constrtype, idxname
                    FROM sysconstraints
                    WHERE tabid = ?
                    AND constrtype IN ('P', 'U')
                    """,
                    (tabid,),
                )
                constraint_map: Dict[str, str] = {}
                rows = cursor.fetchall()
                logger.info(f"[IFX][IDX] sysconstraints P/U rows={len(rows)}")
                for row in rows:
                    constrtype = row[0].strip() if row[0] else None
                    idxname = row[1].strip() if row[1] else None
                    if constrtype and idxname:
                        constraint_map[idxname] = constrtype

                results: List[Dict[str, Any]] = []
                for row in index_rows:
                    idxname = (row[0] or "").strip()
                    idxtype = (row[1] or "").strip()

                    parts = [part for part in row[2:] if part is not None and int(part) > 0]
                    cols: List[str] = []
                    for colno in parts:
                        colname = col_map.get(int(colno))
                        if colname:
                            cols.append(colname)

                    if not cols:
                        continue

                    constrtype = constraint_map.get(idxname)
                    is_primary = constrtype == "P"
                    is_unique = is_primary or (constrtype == "U")

                    results.append(
                        {
                            "name": idxname,
                            "unique": bool(is_unique),
                            "primary": bool(is_primary),
                            "columns": cols,
                            "source_idxtype": idxtype,
                        }
                    )

                logger.info(f"[IFX][IDX] returning {len(results)} index defs")
                return results

            return self._run_operation_with_retry(f"get_table_indexes:{lookup_no_quotes}", _get_indexes)
        except Exception as e:
            logger.error(f"Error getting indexes for Informix table {table_name}: {e}")
            return []
