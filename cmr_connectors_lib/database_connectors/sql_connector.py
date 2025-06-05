#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import abstractmethod

class SqlConnector():

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.driver = None

    
    @abstractmethod
    def get_connection(self):
        pass
    
    @abstractmethod
    def ping(self):
        pass
    
    @abstractmethod
    def get_connection_tables(self):
        pass
    
    def get_connection_columns(self, table_name):
        pass

    
    def get_database_schema(self):
        pass
