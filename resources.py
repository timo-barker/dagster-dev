import dagster as dg
import pyodbc
import os

@dg.resource(
    description="SQL Server source database connection"
)
def sql_server_source(context: dg.AssetExecutionContext):
    """A Dagster resource that provides a SQL Server connection."""
    server = context.resource_config["server"]
    database = context.resource_config["database"]
    user = context.resource_config["user"]
    password = context.resource_config["password"]
    port = context.resource_config["port"]

    def get_connection():
        """Returns a pyodbc connection to the SQL Server database."""
        return pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={user};PWD={password}")
    return get_connection

@dg.resource(
    description="SQL Server target database connection"
)
def sql_server_target(context: dg.AssetExecutionContext):
    """A Dagster resource that provides a SQL Server connection."""
    server = context.resource_config["server"]
    database = context.resource_config["database"]
    user = context.resource_config["user"]
    password = context.resource_config["password"]
    port = context.resource_config["port"]

    def get_connection():
        """Returns a pyodbc connection to the SQL Server database."""
        #return sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)
        return pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={user};PWD={password}")
    return get_connection

@dg.definitions
def defs():
    """Defines the Dagster resources for this project. """
    return dg.Definitions(
        resources={
            "sql_server_source": sql_server_source.configured({
                "server": os.getenv("SQL_SERVER_SRC_HOST"),
                "database": os.getenv("SQL_SERVER_SRC_DATABASE"),
                "user": os.getenv("SQL_SERVER_SRC_USER"),
                "password": os.getenv("SQL_SERVER_SRC_PASSWORD"),
                "port": os.getenv("SQL_SERVER_SRC_PORT")
            }),
            "sql_server_target": sql_server_target.configured({
                "server": os.getenv("SQL_SERVER_TGT_HOST"),
                "database": os.getenv("SQL_SERVER_TGT_DATABASE"),
                "user": os.getenv("SQL_SERVER_TGT_USER"),
                "password": os.getenv("SQL_SERVER_TGT_PASSWORD"),
                "port": os.getenv("SQL_SERVER_TGT_PORT")
            }),
        }
    )
