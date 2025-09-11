import dagster as dg
import polars as pl
import time
from dagster_tutorial.defs.assets import constants
from dagster_tutorial.defs.partitions import monthly_partition
from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_partition_range(partition_key, period='monthly'):
    """Helper function to return the start and end date strings for a given partition key."""
    if partition_key:
        start_date = datetime.strptime(partition_key, constants.DATE_FORMAT).replace(day=1)
        end_date = start_date + relativedelta(months=1)
        start_date_str = start_date.strftime(constants.DATE_FORMAT)
        end_date_str = end_date.strftime(constants.DATE_FORMAT)
    else:
        start_date_str, end_date_str = '', ''

    return start_date_str, end_date_str

def get_customer_key_range(sql_server_source, partition_key=None):
    """Retrieves the minimum, maximum, and count of CustomerKey from the CustomerStaging table."""
    start_date_str, end_date_str = get_partition_range(partition_key)

    with sql_server_source() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT MIN(CustomerID), MAX(CustomerID), COUNT(CustomerID)
                FROM Sales.Customer
                WHERE (    ModifiedDate >= '{start_date_str}'
                       AND ModifiedDate <  '{end_date_str}')
                   OR NULLIF('{start_date_str}','') IS NULL
                """
            )
            min_key, max_key, cnt_key = cursor.fetchone()
            return min_key, max_key, cnt_key

def fetch_data(conn, query):
    """Helper function to fetch data into a Polars DataFrame using pl.read_database"""
    return pl.read_database(query, connection=conn)

def process_customer_batch(sql_server_source, sql_server_target, partition_key, batch_start, batch_end):
    """Processes a batch of customer records, performing Type 1 SCD merge (inserts, updates, and deletes)."""
    start_date_str, end_date_str = get_partition_range(partition_key)
    inserts, updates, deletes, ignores, records = 0, 0, 0, 0, 0

    source_query = f"""
        SELECT CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, rowguid, ModifiedDate
        FROM Sales.Customer
        WHERE CustomerID >= {batch_start} AND CustomerID <= {batch_end}
          AND (   (    ModifiedDate >= '{start_date_str}'
                   AND ModifiedDate <  '{end_date_str}')
               OR NULLIF('{start_date_str}','') IS NULL)
    """

    target_query = f"""
        SELECT CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, rowguid, ModifiedDate
        FROM dbo.Customer
        WHERE CustomerID >= {batch_start} AND CustomerID <= {batch_end}
    """

    with sql_server_source() as conn_source:
        source_df = fetch_data(conn_source, source_query)

    with sql_server_target() as conn_target:
        target_df = fetch_data(conn_target, target_query)

    source_ids = source_df["CustomerID"].to_list()
    target_ids = target_df["CustomerID"].to_list()
    insert_ids = list(set(source_ids) - set(target_ids))
    update_ids = list(set(source_ids).intersection(set(target_ids)))
    delete_ids = list(set(target_ids) - set(source_ids))

    # Inserts
    if insert_ids:
        insert_df = source_df.filter(pl.col("CustomerID").is_in(insert_ids))
        inserts = insert_df.height
        insert_data = insert_df.to_numpy().tolist()

        with sql_server_target() as conn_target:
            with conn_target.cursor() as cursor_target:
                cursor_target.fast_executemany = True
                cursor_target.executemany(
                    """
                    INSERT INTO dbo.Customer (CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, rowguid, ModifiedDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_data,
                )
                conn_target.commit()

    # Updates
    if update_ids:
        update_df = source_df.filter(pl.col("CustomerID").is_in(update_ids))
        target_update_df = target_df.filter(pl.col("CustomerID").is_in(update_ids))

        join_condition = (pl.col("PersonID") != pl.col("PersonID_target")) | \
                         (pl.col("StoreID") != pl.col("StoreID_target")) | \
                         (pl.col("TerritoryID") != pl.col("TerritoryID_target")) | \
                         (pl.col("AccountNumber") != pl.col("AccountNumber_target")) | \
                         (pl.col("rowguid") != pl.col("rowguid_target")) | \
                         (pl.col("ModifiedDate") != pl.col("ModifiedDate_target"))

        diff_df = update_df.join(target_update_df, on="CustomerID", how="left", suffix="_target").filter(join_condition)
        updates = diff_df.height

        if updates > 0:
            update_rows_df = diff_df.select(
                [
                    pl.col("PersonID"),
                    pl.col("StoreID"),
                    pl.col("TerritoryID"),
                    pl.col("AccountNumber"),
                    pl.col("rowguid"),
                    pl.col("ModifiedDate"),
                    pl.col("CustomerID"),
                ]
            ).to_numpy().tolist()

            with sql_server_target() as conn_target:
                with conn_target.cursor() as cursor_target:
                    cursor_target.executemany(
                        """
                        UPDATE dbo.Customer
                        SET PersonID = ?, StoreID = ?, TerritoryID = ?, AccountNumber = ?, rowguid = ?, ModifiedDate = ?
                        WHERE CustomerID = ?
                        """,
                        update_rows_df,
                    )
                    conn_target.commit()

    # Deletes
    if delete_ids:
        delete_df = target_df.filter(pl.col("CustomerID").is_in(delete_ids))
        deletes = delete_df.height
        delete_data = delete_df.select([pl.col("CustomerID")]).to_numpy().tolist()

        with sql_server_target() as conn_target:
            with conn_target.cursor() as cursor_target:
                cursor_target.executemany(
                    """
                    DELETE dbo.Customer
                    WHERE CustomerID = ?
                    """,
                    delete_data,
                )
                conn_target.commit()

    ignores = len(target_ids) - updates - len(delete_ids)
    records = inserts + updates + ignores

    return inserts, updates, deletes, ignores, records

@dg.asset(
    description="dbo.Customer",
    required_resource_keys={"sql_server_source", "sql_server_target"},
    partitions_def=monthly_partition,
    group_name="MyDatabase",
    kinds={"sqlserver", "table"},
)
def customer(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Processes customer data from CustomerStaging to Customer, tracking metadata."""
    start_time = time.time()
    partition_key = context.partition_key
    sql_server_source = context.resources.sql_server_source
    sql_server_target = context.resources.sql_server_target
    batch_size = constants.BATCH_SIZE
    min_key, max_key, cnt_key = get_customer_key_range(sql_server_source, partition_key)
    inserts, updates, deletes, ignores, records = 0, 0, 0, 0, 0

    if min_key is None or max_key is None or cnt_key is None:
        context.log.info("CustomerStaging table is empty, skipping processing.")
        end_time = time.time()
        duration = end_time - start_time
        return dg.MaterializeResult(
            metadata={
                "INSERT": dg.MetadataValue.int(0),
                "UPDATE": dg.MetadataValue.int(0),
                "DELETE": dg.MetadataValue.int(0),
                "IGNORE": dg.MetadataValue.int(0),
                "MINKEY": dg.MetadataValue.int(0),
                "MAXKEY": dg.MetadataValue.int(0),
                "BTCHSZ": dg.MetadataValue.int(batch_size),
                "ROWCNT": dg.MetadataValue.int(0),
                "SECOND": dg.MetadataValue.float(round(duration, 4)),
                "RCRD_S": dg.MetadataValue.int(0),
            }
        )

    processed_count = 0 
    for batch_start in range(min_key, max_key + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, max_key)
        expected_records_in_batch = min(batch_size, cnt_key - processed_count)

        if expected_records_in_batch <= 0:
            context.log.info("No more data to process. Skipping remaining batches.")
            break

        i, u, d, x, r = process_customer_batch(sql_server_source, sql_server_target, partition_key, batch_start, batch_end)
        inserts += i
        updates += u
        deletes += d
        ignores += x
        records += r
        processed_count += r  
        context.log.info(f"Processed batch from {batch_start} to {batch_end}")

    end_time = time.time()
    duration = end_time - start_time
    records_per_second = records / duration if duration > 0 else 0

    return dg.MaterializeResult(
        metadata={
            "INSERT": dg.MetadataValue.int(inserts),
            "UPDATE": dg.MetadataValue.int(updates),
            "DELETE": dg.MetadataValue.int(deletes),
            "IGNORE": dg.MetadataValue.int(ignores),
            "MINKEY": dg.MetadataValue.int(min_key if min_key is not None else 0),
            "MAXKEY": dg.MetadataValue.int(max_key if max_key is not None else 0),
            "BTCHSZ": dg.MetadataValue.int(batch_size),
            "ROWCNT": dg.MetadataValue.int(records),
            "SECOND": dg.MetadataValue.float(round(duration, 4)),
            "RCRD_S": dg.MetadataValue.int(int(round(records_per_second, 0))),
        }
    )
