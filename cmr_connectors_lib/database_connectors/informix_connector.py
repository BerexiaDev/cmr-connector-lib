#!/usr/bin/python
# -*- coding: utf-8 -*-

from .sql_connector import SqlConnector
from sqlalchemy import create_engine


class InformixConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "informix+informixdb"

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            if "first" not in query:
                query = query.replace(";", " ")
                query = f"SELECT FIRST {rows} * FROM ({query})"
        return query

    def get_engine(self):
        informix_connection_string = '{driver}://{username}:{password}@{hostname}:{port}/{database}'
        engine = create_engine(
            informix_connection_string.format(
                driver=self.driver,
                username=self.user,
                password=self.password,
                hostname=self.host,
                port=self.port,
                database=self.database,
            )
        )
        return engine