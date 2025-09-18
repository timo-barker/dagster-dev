import dagster as dg
import pyodbc
import os
from getpass import getpass

@dg.resource(
    description="SQL Server source database connection"
)
def sql_server_source(context: dg.AssetExecutionContext):
    """A Dagster resource that provides a SQL Server connection."""
    server = context.resource_config["server"]
    database = context.resource_config["database"]
    port = context.resource_config["port"]

    def get_connection():
        """Returns a pyodbc connection to the SQL Server database."""
        return pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;")
    return get_connection

@dg.resource(
    description="SQL Server target database connection"
)
def sql_server_target(context: dg.AssetExecutionContext):
    """A Dagster resource that provides a SQL Server connection."""
    server = context.resource_config["server"]
    database = context.resource_config["database"]

    def get_connection():
        """Returns a pyodbc connection to the SQL Server database."""
        # return sqlalchemy.create_engine(
        #     f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server", 
        #     fast_executemany=True
        #     )
        return pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
        )
    return get_connection

@dg.resource(
    description="Teradata source database connection"
)
def teradata_source(context: dg.AssetExecutionContext):
    """A Dagster resource that provides a Teradata connection."""
    server = context.resource_config["server"]
    username = getpass("Enter your username: ")
    password = getpass("Enter your password: ")

    def get_connection():
        """Returns a pyodbc connection to the Teradata database."""
        return pyodbc.connect(
            "DRIVER={Teradata Database ODBC Driver 20.00};"
            f"DBCNAME={server};"
            f"UID={username};"
            f"PWD={password};"
            "Authentication=LDAP;"
        )
    return get_connection

@dg.definitions
def defs():
    """Defines the Dagster resources for this project. """
    return dg.Definitions(
        resources={
            "sql_server_source": sql_server_source.configured({
                "server": os.getenv("SQL_SERVER_SRC_HOST"),
                "database": os.getenv("SQL_SERVER_SRC_DATABASE"),
                "port": os.getenv("SQL_SERVER_SRC_PORT")
            }),
            "sql_server_target": sql_server_target.configured({
                "server": os.getenv("SQL_SERVER_TGT_HOST"),
                "database": os.getenv("SQL_SERVER_TGT_DATABASE"),
            }),
            "teradata_source": teradata_source.configured({
                "server": os.getenv("TERADATA_SRC_HOST"),
            }),
        }
    )
