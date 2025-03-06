#!/usr/bin/python
# -*- coding: utf-8 -*-

from urllib.parse import quote
import pandas as pd
from sqlalchemy import create_engine

from connectors.connector import Connector


class SqlConnector(Connector):

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.driver = None

    def get_engine(self):
        engine = create_engine(
            f"{self.driver}://{quote(self.user, safe='/')}:{quote(self.password, safe='/')}@{self.host}:{self.port}"
            f"/{self.database}", echo=False)

        return engine

    def get_df(self, query, preview=False, rows=10, *args, **kwargs):
        constructed_query = self.construct_query(query, preview, rows)
        conn = self.get_engine()
        return pd.read_sql_query(constructed_query, conn)

    def upload_df(self, df, table, schema, if_exists='fail', *args, **kwargs):
        conn = self.get_engine()
        df.to_sql(table, con=conn, index=False, if_exists=if_exists)

    def construct_query(self, query, preview, rows):
        return query