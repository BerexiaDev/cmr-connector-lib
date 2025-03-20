#!/usr/bin/python
# -*- coding: utf-8 -*-

from .sql_connector import SqlConnector
from sqlalchemy import create_engine


class InformixConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, protocol):
        super().__init__(host, user, password, port, database)
        self.driver = "pyodbc"
        self.protocol = protocol  # Required for Informix
        self.driver_path = "/home/soufiane/Bureau/cmr-etl/cmr_driver/ddifcl28.so"

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            if "first" not in query:
                query = query.replace(";", " ")
                query = f"SELECT FIRST {rows} * FROM ({query})"
        return query

    def get_engine(self):
        informix_connection_string =  (
            f"DRIVER={self.driver};"
            f"DATABASE={self.database};"
            f"HOSTNAME={self.host};"
            f"PORT={self.port};"
            f"PROTOCOL={self.protocol};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"DRIVER_PATH={self.driver_path};"
        )
        engine = create_engine(
            informix_connection_string.format(
                driver=self.driver,
                username=self.user,
                password=self.password,
                hostname=self.host,
                port=self.port,
                database=self.database,
                protocol=self.protocol,
                driver_path=self.driver_path
            )
        )
        return engine
