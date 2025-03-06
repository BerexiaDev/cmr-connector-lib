#!/usr/bin/python
# -*- coding: utf-8 -*-

from .sql_connector import SqlConnector


class SqlServerConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "mssql+pymssql"

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            query = query.replace("select", "select " + " top " + str(rows))
        return query
