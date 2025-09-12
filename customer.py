import dagster as dg
import polars as pl
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from . import constants
from ..partitions import monthly_partition

def get_partition_range(partition_key, period="monthly"):
    """Helper function to return the start and end date strings for a given partition key."""
    if partition_key:
        start_date = datetime.strptime(partition_key, constants.DATE_FORMAT).replace(day=1)
        if period == "monthly":
            end_date = start_date + relativedelta(months=1)
        elif period == "weekly":
            end_date = start_date + relativedelta(weeks=1)
        elif period == "daily":
            end_date = start_date + relativedelta(days=1)
        else:
            raise ValueError(f"Invalid period: {period}")
        start_date_str = start_date.strftime(constants.DATE_FORMAT)
        end_date_str = end_date.strftime(constants.DATE_FORMAT)
    else:
        start_date_str, end_date_str = "", ""

    return start_date_str, end_date_str

def fetch_data(conn, query):
    """Helper function to fetch data into a Polars DataFrame using pl.read_database"""
    return pl.read_database(query, connection=conn)

def all_to_str(df: pl.DataFrame) -> pl.DataFrame:
    """Helper function to convert numeric columns in a Polars DataFrame to string type."""
    cols = []
    for c in df.columns:
        if df[c].dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64]:
            cols.append(pl.col(c).cast(pl.Utf8, strict=False).alias(c))
        else:
            cols.append(pl.col(c))
    return df.select(cols), df.height

@dg.asset(
    description="Staging asset for dbo.Customer",
    required_resource_keys={"sql_server_source", "sql_server_target"},
    partitions_def=monthly_partition,
    group_name="MyDatabase",
    kinds={"sqlserver", "table"},
)
def customer(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Extracts customer data from Sales.Customer into staging table stg.Customer."""
    start_time = time.time()
    partition_key = context.partition_key
    sql_server_source = context.resources.sql_server_source
    sql_server_target = context.resources.sql_server_target
    start_date_str, end_date_str = get_partition_range(partition_key, "monthly")

    source_query = f"""
        SELECT CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, rowguid, ModifiedDate
        FROM Sales.Customer
        WHERE (    ModifiedDate >= '{start_date_str}'
               AND ModifiedDate <  '{end_date_str}')
        --OR NULLIF('{start_date_str}','') IS NULL
    """

    with sql_server_source() as conn_source:
        df, rows_loaded = all_to_str(fetch_data(conn_source, source_query))

    with sql_server_target() as conn_target:
        with conn_target.cursor() as cursor_target:
            cursor_target.execute("TRUNCATE TABLE stg.Customer")
            if rows_loaded > 0:
                insert_data = df.to_numpy().tolist()
                cursor_target.fast_executemany = True
                cursor_target.executemany(
                    """
                    INSERT INTO stg.Customer (CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, rowguid, ModifiedDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_data,
                )
            conn_target.commit()

    end_time = time.time()
    duration = end_time - start_time
    records_per_second = rows_loaded / duration if duration > 0 else 0

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(rows_loaded),
            "duration_seconds": dg.MetadataValue.float(round(duration, 4)),
            "rows_per_second": dg.MetadataValue.int(int(round(records_per_second, 0))),
        }
    )
