#!/usr/bin/python
# -*- coding: utf-8 -*-

from .sql_connector import SqlConnector


class PostgresConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "postgresql+psycopg2"

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            query = query.replace(";", " ")
            query += " limit " + str(rows)
        return query