import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import oracledb

from cmr_connectors_lib.connectors_factory import ConnectorFactory
from cmr_connectors_lib.database_connectors.oracle_connector import OracleConnector
from cmr_connectors_lib.database_connectors.sql_connector import is_retryable_connection_error
from cmr_connectors_lib.database_connectors.sql_connector_utils import (
    cast_oracle_to_postgresql_type,
    cast_oracle_to_typescript,
)

SETTINGS = {"host": "db", "user": "app_user", "password": "pwd", "port": 1521, "database": "orclpdb1"}


def make_connector(cursor, schema="sales"):
    connector = OracleConnector("db", "app_user", "pwd", 1521, "orclpdb1", schema)
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector._open_connection = Mock(return_value=connection)
    connector._retry_delay = lambda attempt: 0
    return connector


class FactoryTests(unittest.TestCase):
    def test_creates_oracle_connector_with_schema(self):
        connector = ConnectorFactory().create_connector("oracle", {**SETTINGS, "schema": "sales"})
        self.assertIsInstance(connector, OracleConnector)
        self.assertEqual(connector.schema, "SALES")
        self.assertEqual(connector.driver, "oracledb")

    def test_schema_defaults_to_login_user(self):
        connector = ConnectorFactory().create_connector("oracle", dict(SETTINGS))
        self.assertEqual(connector.schema, "APP_USER")


class IdentifierTests(unittest.TestCase):
    def setUp(self):
        self.connector = OracleConnector("db", "app_user", "pwd", 1521, "orclpdb1", "sales")

    def test_lowercase_reference_names_resolve_to_uppercase_catalog_names(self):
        self.assertEqual(self.connector._qualify_table("customers"), '"SALES"."CUSTOMERS"')

    def test_owner_prefix_overrides_configured_schema(self):
        self.assertEqual(self.connector._qualify_table("hr.employees"), '"HR"."EMPLOYEES"')

    def test_quotes_are_escaped(self):
        self.assertEqual(self.connector._quote_identifier('a"b'), '"A""B"')


class TypeMappingTests(unittest.TestCase):
    def test_typescript_types(self):
        self.assertEqual(cast_oracle_to_typescript("NUMBER"), "number")
        self.assertEqual(cast_oracle_to_typescript("VARCHAR2"), "string")
        self.assertEqual(cast_oracle_to_typescript("DATE"), "Date")
        self.assertEqual(cast_oracle_to_typescript("TIMESTAMP(6)"), "Datetime")
        self.assertEqual(cast_oracle_to_typescript("TIMESTAMP(6) WITH TIME ZONE"), "Datetime")
        self.assertEqual(cast_oracle_to_typescript("BLOB"), "any")
        self.assertEqual(cast_oracle_to_typescript(None), "any")

    def test_postgres_types(self):
        self.assertEqual(cast_oracle_to_postgresql_type("NUMBER"), "NUMERIC")
        self.assertEqual(cast_oracle_to_postgresql_type("VARCHAR2"), "VARCHAR")
        self.assertEqual(cast_oracle_to_postgresql_type("DATE"), "TIMESTAMP")
        self.assertEqual(cast_oracle_to_postgresql_type("TIMESTAMP(9)"), "TIMESTAMP")
        self.assertEqual(cast_oracle_to_postgresql_type("TIMESTAMP(6) WITH LOCAL TIME ZONE"), "TIMESTAMPTZ")
        self.assertEqual(cast_oracle_to_postgresql_type("INTERVAL DAY(2) TO SECOND(6)"), "INTERVAL")
        self.assertEqual(cast_oracle_to_postgresql_type("CLOB"), "TEXT")
        self.assertEqual(cast_oracle_to_postgresql_type("SOMETHING_ELSE"), "TEXT")


class DefaultValueTests(unittest.TestCase):
    def test_only_literal_defaults_are_kept(self):
        portable = OracleConnector._portable_default
        self.assertEqual(portable("0 "), "0")
        self.assertEqual(portable("'N'"), "'N'")
        self.assertEqual(portable("-1.5"), "-1.5")
        self.assertIsNone(portable("SYSDATE"))
        self.assertIsNone(portable("SYS_GUID()"))
        self.assertIsNone(portable('"ISEQ$$_1".nextval'))
        self.assertIsNone(portable(None))


class CatalogTests(unittest.TestCase):
    def test_ping_checks_schema_visibility(self):
        cursor = MagicMock()
        cursor.fetchone.side_effect = [(1,), (1,)]
        self.assertTrue(make_connector(cursor).ping())
        cursor.execute.assert_any_call("SELECT COUNT(*) FROM all_users WHERE username = :1", ["SALES"])

    def test_ping_fails_when_schema_is_missing(self):
        cursor = MagicMock()
        cursor.fetchone.side_effect = [(1,), (0,)]
        self.assertFalse(make_connector(cursor).ping())

    def test_tables_are_listed_for_configured_schema(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("CUSTOMERS",), ("ORDERS",)]
        self.assertEqual(make_connector(cursor).get_connection_tables(), ["CUSTOMERS", "ORDERS"])
        self.assertEqual(cursor.execute.call_args[0][1], ["SALES"])

    def test_columns_use_uppercase_catalog_lookup(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("ID", "NUMBER"), ("CREATED_AT", "TIMESTAMP(6)")]
        columns = make_connector(cursor).get_connection_columns("customers")
        self.assertEqual(columns, [{"name": "ID", "type": "number"}, {"name": "CREATED_AT", "type": "Datetime"}])
        self.assertEqual(cursor.execute.call_args[0][1], ["SALES", "CUSTOMERS"])

    def test_extract_table_schema_maps_types_and_defaults(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (1, "ID", "NUMBER", 22, "NO", None, "YES", "NO", "YES"),
            (2, "CREATED", "DATE", 7, "YES", "SYSDATE ", "NO", "NO", "NO"),
            (3, "STATUS", "VARCHAR2", 10, "YES", "'NEW'", "NO", "NO", "NO"),
        ]
        schema = make_connector(cursor).extract_table_schema("customers")
        self.assertEqual([c["type"] for c in schema], ["NUMERIC", "TIMESTAMP", "VARCHAR"])
        self.assertEqual([c["default"] for c in schema], [None, None, "'NEW'"])
        self.assertEqual(schema[0]["primary_key"], "YES")

    def test_indexes_flag_primary_key(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("PK_CUSTOMERS", "ID", "UNIQUE", 1),
            ("IX_NAME", "LAST_NAME", "NONUNIQUE", 0),
            ("IX_NAME", "FIRST_NAME", "NONUNIQUE", 0),
        ]
        indexes = make_connector(cursor).get_table_indexes("customers")
        self.assertEqual(indexes, [
            {"name": "PK_CUSTOMERS", "unique": True, "primary": True, "columns": ["ID"]},
            {"name": "IX_NAME", "unique": False, "primary": False, "columns": ["LAST_NAME", "FIRST_NAME"]},
        ])


class DataTests(unittest.TestCase):
    def test_count_and_batch_queries_are_qualified(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (42,)
        connector = make_connector(cursor)
        self.assertEqual(connector.count_table_rows("customers"), 42)
        cursor.execute.assert_called_with('SELECT COUNT(*) FROM "SALES"."CUSTOMERS"')

        cursor.fetchall.return_value = [(1,)]
        connector.fetch_batch(None, "customers", 100, 50)
        cursor.execute.assert_called_with('SELECT * FROM "SALES"."CUSTOMERS" OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY')

    def test_stream_batch_yields_until_exhausted(self):
        cursor = MagicMock()
        cursor.fetchmany.side_effect = [[(1,), (2,)], [(3,)], []]
        batches = list(make_connector(cursor).stream_batch(None, "customers", 2))
        self.assertEqual(batches, [[(1,), (2,)], [(3,)]])
        cursor.execute.assert_called_once_with('SELECT * FROM "SALES"."CUSTOMERS"', [])

    def test_stream_batch_retries_transport_error_before_first_rows(self):
        cursor = MagicMock()
        lost = oracledb.OperationalError("DPY-4011: the database or network closed the connection")
        cursor.fetchmany.side_effect = [lost, [(1,)], []]
        connector = make_connector(cursor)
        with patch("cmr_connectors_lib.database_connectors.oracle_connector.time.sleep"):
            self.assertEqual(list(connector.stream_batch(None, "customers", 10)), [[(1,)]])
        self.assertEqual(connector._open_connection.call_count, 2)

    def test_stream_batch_raises_sql_errors(self):
        cursor = MagicMock()
        cursor.execute.side_effect = oracledb.DatabaseError("ORA-00942: table or view does not exist")
        with self.assertRaises(oracledb.DatabaseError):
            list(make_connector(cursor).stream_batch(None, "missing", 10))

    def test_fetch_deltas_binds_positionally_and_drops_row_number(self):
        cursor = MagicMock()
        cursor.description = [("ID",), ("operation",), ("RN",)]
        cursor.fetchall.side_effect = [[(1, "insertion", 1)], []]
        since = datetime(2026, 1, 1)
        rows = list(make_connector(cursor).fetch_deltas(None, ["ID"], "customers_log", since, batch_size=500))
        self.assertEqual(rows, [{"ID": 1, "operation": "insertion"}])
        self.assertEqual(cursor.execute.call_args_list[0][0][1], [since, 0, 500])
        self.assertIn('FROM "SALES"."CUSTOMERS_LOG" t', cursor.execute.call_args_list[0][0][0])


class RetryClassificationTests(unittest.TestCase):
    def test_oracle_transport_errors_are_retryable(self):
        self.assertTrue(is_retryable_connection_error(Exception("ORA-03113: end-of-file on communication channel")))
        self.assertTrue(is_retryable_connection_error(Exception("DPY-4011: the database or network closed the connection")))

    def test_oracle_sql_errors_are_not_retryable(self):
        self.assertFalse(is_retryable_connection_error(Exception("ORA-00942: table or view does not exist")))
        self.assertFalse(is_retryable_connection_error(Exception("ORA-01017: invalid username/password")))


if __name__ == "__main__":
    unittest.main()
