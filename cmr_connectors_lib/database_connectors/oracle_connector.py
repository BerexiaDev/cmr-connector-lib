#!/usr/bin/python
# -*- coding: utf-8 -*-

from .sql_connector import SqlConnector
from sqlalchemy import create_engine
import cx_Oracle


class OracleConnector(SqlConnector):

    mode=None
    database_type=None

    def __init__(self, host=None, user=None, password=None, port=None, database=None, mode=None, database_type=None, **kwargs):
        super().__init__(host, user, password, port, database)
        self.mode = mode
        self.database_type = database_type

        self.driver = "oracle+cx_oracle"
        try:
            lib_dir = r"C:\Oracle\instantclient_19_11"
            cx_Oracle.init_oracle_client(lib_dir=lib_dir)
        except Exception as err:
            print("Error connecting: cx_Oracle.init_oracle_client()")
            print(err)
            # sys.exit(1);

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            if "where" in query:
                query = query.replace("where", " where rownum <=  " + str(rows) + "AND ")
            else:
                query = query.replace(";", " ")
                query += " where rownum <= " + str(rows)
        return query

    def get_engine(self):
        if self.database_type == 'service_name':
            oracle_connection_string = (
                    '{driver}://{username}:{password}@' +
                    '{hostname}:{port}/?service_name={service_name}&mode={mode}'
            )
            engine = create_engine(
                oracle_connection_string.format(
                    driver=self.driver,
                    username=self.user,
                    password=self.password,
                    hostname=self.host,
                    port=self.port,
                    service_name=self.database,
                    mode=self.mode,
                )
            )

        else:
            oracle_connection_string = '{driver}://{username}:{password}@{hostname}:{port}/{database}'
            engine = create_engine(
                oracle_connection_string.format(
                    driver=self.driver,
                    username=self.user,
                    password=self.password,
                    hostname=self.host,
                    port=self.port,
                    database=self.database,
                )
            )

        return engine

