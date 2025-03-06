from database_connectors.oracle_connector import OracleConnector
from database_connectors.postgres_connector import PostgresConnector
from database_connectors.sql_server_connector import SqlServerConnector


class ConnectorFactory():
    """Helper class that provides a standard way to create a Data Checker using factory method"""
    
    def __init__(self):
        pass
    
    def create_connector(self, connector_type, connector_settings):

        if connector_type == 'sqlserver':
            connector = SqlServerConnector(connector_settings["host"], connector_settings["user"],
                                           connector_settings["password"],connector_settings["port"],
                                           connector_settings["database"])
            return connector

        elif connector_type == 'postgres':
            connector = PostgresConnector(connector_settings["host"], connector_settings["user"],
                                          connector_settings["password"], connector_settings["port"],
                                          connector_settings["database"])
            return connector
        elif connector_type == 'oracledb':
            connector = OracleConnector(**connector_settings)
            return connector
    
    