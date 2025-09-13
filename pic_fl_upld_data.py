import dagster as dg
import polars as pl
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil.relativedelta import relativedelta
from .. import constants


@dg.asset(
    required_resource_keys={"sql_server_source", "sql_server_target"},
    deps=["pic_fl_upld_log"],
    description="irb.PIC_FL_UPLD_DATA",
    kinds={"sqlserver", "table"},
    group_name="pic_automation",
)
def pic_fl_upld_data(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """SCD Type 1 merge between source and target PIC_FL_UPLD_DATA tables."""

    def __get_partition_range(partition_key: str | None = None) -> tuple[str, str]:
        """Converts partition key into date range strings."""
        if partition_key:
            start_date = datetime.strptime(partition_key, constants.DATE_FORMAT)
            end_date = start_date + relativedelta(days=1)
            return start_date.strftime(constants.DATE_FORMAT), end_date.strftime(
                constants.DATE_FORMAT
            )
        return "", ""

    def __fetch_data(conn, query) -> pl.DataFrame:
        """Executes SQL query and returns results as Polars DataFrame."""
        return pl.read_database(query, connection=conn)

    def _get_pic_fl_upld_data_key_range(
        sql_server_source, batch_size: int, partition_key: str | None = None
    ) -> pl.DataFrame:
        """Gets IDENT ranges from source table divided into batches."""
        start_date_str, end_date_str = __get_partition_range(partition_key)
        range_query = f"""
            WITH _ (IDENT, ROW_NBR) AS (
                SELECT IDENT
                      ,ROW_NUMBER() OVER(ORDER BY IDENT)
                FROM irb.PIC_FL_UPLD_DATA
                WHERE (    UPDATED_DTTM >= '{start_date_str}'
                       AND UPDATED_DTTM <  '{end_date_str}')
                   OR NULLIF('{start_date_str}','') IS NULL
            )
            SELECT ((ROW_NBR-1)/{batch_size}) AS row_nbr
                  ,MIN(IDENT) AS min_key
                  ,MAX(IDENT) AS max_key
            FROM _
            GROUP BY ((ROW_NBR-1)/{batch_size})
            """
        with sql_server_source() as conn:
            range_df = __fetch_data(conn, range_query)
        return range_df

    def _coalesce_ranges(
        src_ranges: pl.DataFrame, tgt_ranges: pl.DataFrame
    ) -> pl.DataFrame:
        """Combines source and target key range gaps/islands with head/tail preservation."""
        empty_schema = {"row_nbr": pl.Int64, "min_key": pl.Int64, "max_key": pl.Int64}

        if src_ranges is None or src_ranges.is_empty():
            if tgt_ranges is None or tgt_ranges.is_empty():
                return pl.DataFrame(
                    {"row_nbr": [], "min_key": [], "max_key": []}, schema=empty_schema
                )

            base = tgt_ranges.select("min_key", "max_key").sort("min_key")
            return (
                base.with_row_index(name="row_nbr")
                .with_columns(pl.col("row_nbr").cast(pl.Int64))
                .select(["row_nbr", "min_key", "max_key"])
            )

        src = src_ranges.select(
            pl.col("row_nbr").cast(pl.Int64, strict=False),
            pl.col("min_key").cast(pl.Int64, strict=False),
            pl.col("max_key").cast(pl.Int64, strict=False),
        )
        tgt = None

        if tgt_ranges is not None and not tgt_ranges.is_empty():
            tgt = tgt_ranges.select(
                pl.col("min_key").cast(pl.Int64, strict=False),
                pl.col("max_key").cast(pl.Int64, strict=False),
            )

        tails = []
        if tgt is not None and not tgt.is_empty():
            tgt_min = int(tgt["min_key"].min())
            first_src_min = int(src["min_key"].min())
            if tgt_min < first_src_min:
                tails.append(
                    pl.DataFrame(
                        {
                            "row_nbr": [-1],
                            "min_key": [tgt_min],
                            "max_key": [first_src_min - 1],
                        },
                        schema=empty_schema,
                    )
                )

            tgt_max = int(tgt["max_key"].max())
            last_src_max = int(src["max_key"].max())
            if tgt_max > last_src_max:
                tails.append(
                    pl.DataFrame(
                        {
                            "row_nbr": [-1],
                            "min_key": [last_src_max + 1],
                            "max_key": [tgt_max],
                        },
                        schema=empty_schema,
                    )
                )

        combined = pl.concat(
            [src.select(["row_nbr", "min_key", "max_key"])] + tails,
            how="vertical_relaxed",
        ).sort("min_key")

        with_next = combined.with_columns(
            next_min=pl.col("min_key").shift(-1), next_max=pl.col("max_key").shift(-1)
        )

        extended = (
            with_next.with_columns(
                max_key=pl.when(pl.col("next_min").is_not_null())
                .then(pl.col("next_min") - 1)
                .otherwise(pl.col("max_key"))
            )
            .select(["row_nbr", "min_key", "max_key"])
            .sort("min_key")
        )

        return extended

    def _select_batch_data(sql_server, min_key: int, max_key: int) -> pl.DataFrame:
        """Fetch data from PIC_FL_UPLD_DATA for a given IDENT range [min_key, max_key]."""
        query = f"""
            SELECT IDENT
                  ,UPLD_ID
                  ,FLD_NM
                  ,FLD_VL
                  ,CREATED_BY
                  ,CREATED_DTTM
                  ,UPDATED_BY
                  ,UPDATED_DTTM
            FROM irb.PIC_FL_UPLD_DATA
            WHERE IDENT BETWEEN {min_key} AND {max_key}
            """
        with sql_server() as conn:
            data_df = __fetch_data(conn, query)
        return data_df

    def __values_differ(col1: str, col2: str) -> pl.Expr:
        """Compare two columns accounting for NULL values"""
        return (
            (pl.col(col1).is_null() & pl.col(col2).is_not_null())
            | (pl.col(col1).is_not_null() & pl.col(col2).is_null())
            | (
                pl.col(col1).is_not_null()
                & pl.col(col2).is_not_null()
                & (pl.col(col1) != pl.col(col2))
            )
        )

    def _bin_records(
        source_df: pl.DataFrame, target_df: pl.DataFrame
    ) -> tuple[list, list, list, list]:
        """Categorize records into insert, update, delete and ignore bins"""
        source_ids = set(source_df["IDENT"].to_list())
        target_ids = set(target_df["IDENT"].to_list())
        insert_ids = list(source_ids - target_ids)
        potential_update_ids = list(source_ids & target_ids)
        delete_ids = list(target_ids - source_ids)

        if potential_update_ids:
            update_source_df = source_df.filter(
                pl.col("IDENT").is_in(potential_update_ids)
            )
            update_target_df = target_df.filter(
                pl.col("IDENT").is_in(potential_update_ids)
            )

            diff_condition = (
                __values_differ("UPLD_ID", "UPLD_ID_target")
                | __values_differ("FLD_NM", "FLD_NM_target")
                | __values_differ("FLD_VL", "FLD_VL_target")
                | __values_differ("CREATED_BY", "CREATED_BY_target")
                | __values_differ("CREATED_DTTM", "CREATED_DTTM_target")
                | __values_differ("UPDATED_BY", "UPDATED_BY_target")
                | __values_differ("UPDATED_DTTM", "UPDATED_DTTM_target")
            )

            diff_df = update_source_df.join(
                update_target_df, on="IDENT", how="left", suffix="_target"
            ).filter(diff_condition)

            update_ids = diff_df["IDENT"].to_list()
            ignore_ids = list(set(potential_update_ids) - set(update_ids))
        else:
            update_ids = []
            ignore_ids = []

        return insert_ids, update_ids, delete_ids, ignore_ids

    def _num_to_str(df: pl.DataFrame) -> pl.DataFrame:
        """Helper function to convert numeric columns in a Polars DataFrame to string type."""
        cols = []
        for c in df.columns:
            if df[c].dtype in [
                pl.Decimal,
                pl.Float32,
                pl.Float64,
                pl.Int8,
                pl.Int16,
                pl.Int32,
                pl.Int64,
                pl.Int128,
                pl.UInt8,
                pl.UInt16,
                pl.UInt32,
                pl.UInt64,
            ]:
                cols.append(pl.col(c).cast(pl.Utf8, strict=False).alias(c))
            else:
                cols.append(pl.col(c))
        return df.select(cols)

    def _insert_batch_data(source_df: pl.DataFrame, insert_ids: list) -> int:
        """Insert new records into the target table."""
        insert_data_df = source_df.filter(pl.col("IDENT").is_in(insert_ids))
        insert_data_pl = _num_to_str(insert_data_df)
        insert_data_np = insert_data_pl.to_numpy()
        insert_data_ls = insert_data_np.tolist()

        with sql_server_target() as conn_target:
            with conn_target.cursor() as cursor_target:
                cursor_target.execute(
                    "ALTER TABLE irb.PIC_FL_UPLD_DATA DISABLE TRIGGER ALL"
                )
                cursor_target.fast_executemany = True
                cursor_target.executemany(
                    """
                    INSERT INTO irb.PIC_FL_UPLD_DATA 
                      (
                        IDENT
                       ,UPLD_ID
                       ,FLD_NM
                       ,FLD_VL
                       ,CREATED_BY
                       ,CREATED_DTTM
                       ,UPDATED_BY
                       ,UPDATED_DTTM
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_data_ls,
                )
                cursor_target.execute(
                    "ALTER TABLE irb.PIC_FL_UPLD_DATA ENABLE TRIGGER ALL"
                )
                conn_target.commit()

        return len(insert_ids)

    def _update_batch_data(source_df: pl.DataFrame, update_ids: list) -> int:
        """Update existing records in the target table."""
        update_data = source_df.filter(pl.col("IDENT").is_in(update_ids))
        update_data_df = update_data.select(
            [
                pl.col("UPLD_ID"),
                pl.col("FLD_NM"),
                pl.col("FLD_VL"),
                pl.col("CREATED_BY"),
                pl.col("CREATED_DTTM"),
                pl.col("UPDATED_BY"),
                pl.col("UPDATED_DTTM"),
                pl.col("IDENT"),
            ]
        )
        update_data_pl = _num_to_str(update_data_df)
        update_data_np = update_data_pl.to_numpy()
        update_data_ls = update_data_np.tolist()

        with sql_server_target() as conn_target:
            with conn_target.cursor() as cursor_target:
                cursor_target.execute(
                    "ALTER TABLE irb.PIC_FL_UPLD_DATA DISABLE TRIGGER ALL"
                )
                cursor_target.executemany(
                    """
                    UPDATE irb.PIC_FL_UPLD_DATA
                    SET UPLD_ID = ?
                       ,FLD_NM = ?
                       ,FLD_VL = ?
                       ,CREATED_BY = ?
                       ,CREATED_DTTM = ?
                       ,UPDATED_BY = ?
                       ,UPDATED_DTTM = ?
                    WHERE IDENT = ?
                    """,
                    update_data_ls,
                )
                cursor_target.execute(
                    "ALTER TABLE irb.PIC_FL_UPLD_DATA ENABLE TRIGGER ALL"
                )
                conn_target.commit()

        return len(update_ids)

    def _delete_batch_data(delete_ids: list) -> int:
        """Delete records from the target table."""
        delete_data_ls = [(id,) for id in delete_ids]
        with sql_server_target() as conn_target:
            with conn_target.cursor() as cursor_target:
                cursor_target.execute(
                    "ALTER TABLE irb.PIC_FL_UPLD_DATA DISABLE TRIGGER ALL"
                )
                cursor_target.executemany(
                    """
                    DELETE irb.PIC_FL_UPLD_DATA
                    WHERE IDENT = ?
                    """,
                    delete_data_ls,
                )
                cursor_target.execute(
                    "ALTER TABLE irb.PIC_FL_UPLD_DATA ENABLE TRIGGER ALL"
                )
                conn_target.commit()

        return len(delete_ids)

    # Step 0: initialize
    start_time = time.time()
    sql_server_source = context.resources.sql_server_source
    sql_server_target = context.resources.sql_server_target
    batch_size = int(constants.BATCH_SIZE)
    inserts = updates = deletes = ignores = min_key = max_key = row_count = seconds = (
        records_per_second
    ) = batch_count = 0

    # Step 1: get batch interval key ranges from source and target
    with ThreadPoolExecutor(max_workers=2) as pool:
        src_future = pool.submit(
            _get_pic_fl_upld_data_key_range, sql_server_source, batch_size, None
        )
        tgt_future = pool.submit(
            _get_pic_fl_upld_data_key_range, sql_server_target, batch_size, None
        )
        src_ranges, tgt_ranges = src_future.result(), tgt_future.result()

    # Step 2: coalesce intervals
    merged_ranges = _coalesce_ranges(src_ranges, tgt_ranges)
    max_batch = merged_ranges.height

    # Step 3: pull data in batches from source and target
    for batch in merged_ranges.iter_rows(named=True):
        batch_min_key = batch["min_key"]
        batch_max_key = batch["max_key"]

        with ThreadPoolExecutor(max_workers=2) as pool:
            src_future = pool.submit(
                _select_batch_data, sql_server_source, batch_min_key, batch_max_key
            )
            tgt_future = pool.submit(
                _select_batch_data, sql_server_target, batch_min_key, batch_max_key
            )
            source_df, target_df = src_future.result(), tgt_future.result()

        # Step 4: bin rows into DML types
        insert_ids, update_ids, delete_ids, ignore_ids = _bin_records(
            source_df, target_df
        )

        # Steps 5/6/7: perform DML operations
        batch_inserts = _insert_batch_data(source_df, insert_ids) if insert_ids else 0
        batch_updates = _update_batch_data(source_df, update_ids) if update_ids else 0
        batch_deletes = _delete_batch_data(delete_ids) if delete_ids else 0
        batch_ignores = len(ignore_ids)

        # Step 8: print status to console and update accumulators
        context.log.info(
            f"Batch {batch['row_nbr']+1:,} of {max_batch}: "
            f"IDENT {batch_min_key:} to {batch_max_key:} => "
            f"{batch_inserts:,} inserted, "
            f"{batch_updates:,} updated, "
            f"{batch_deletes:,} deleted, "
            f"{batch_ignores:,} unchanged"
        )
        inserts += batch_inserts
        updates += batch_updates
        deletes += batch_deletes
        ignores += batch_ignores

    # Step 9: report overall status
    end_time = time.time()
    min_key = int(merged_ranges.select("min_key").min().item())
    max_key = int(merged_ranges.select("max_key").max().item())
    seconds = round(end_time - start_time, 3)
    row_count = inserts + updates + deletes + ignores
    records_per_second = int(round(row_count / seconds if seconds > 0 else 0, 0))
    batch_count = merged_ranges.height

    return dg.MaterializeResult(
        metadata={
            "MINKEY": dg.MetadataValue.int(min_key),
            "MAXKEY": dg.MetadataValue.int(max_key),
            "BTCHSZ": dg.MetadataValue.int(batch_size),
            "BTCHCT": dg.MetadataValue.int(batch_count),
            "INSERT": dg.MetadataValue.int(inserts),
            "UPDATE": dg.MetadataValue.int(updates),
            "DELETE": dg.MetadataValue.int(deletes),
            "IGNORE": dg.MetadataValue.int(ignores),
            "ROWCNT": dg.MetadataValue.int(row_count),
            "SECOND": dg.MetadataValue.float(seconds),
            "RCRD_S": dg.MetadataValue.int(records_per_second),
        }
    )
