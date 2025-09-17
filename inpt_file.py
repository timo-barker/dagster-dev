import dagster as dg
import getpass
import polars as pl
import socket
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from .. import constants
from ..partitions import daily_partition


@dg.asset(
    required_resource_keys={"sql_server_source", "sql_server_target"},
    deps=["inpt", "prfl_dtl"],
    partitions_def=daily_partition,
    name="inpt_file",
    description="irb.INPT_FILE",
    kinds={"sql"},
    group_name="wps_clnt_grt",
)
def inpt_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Upserts records between source and target INPT_FILE tables."""

    def __get_partition_range(partition_key: str | None = None) -> tuple[str, str]:
        """Converts partition key into date range strings."""
        if partition_key:
            start_date, end_date = context.partition_time_window
            return start_date.strftime(constants.DATE_FORMAT), end_date.strftime(
                constants.DATE_FORMAT
            )
        return "", ""

    def __fetch_data(conn, query) -> pl.DataFrame:
        """Executes SQL query and returns results as Polars DataFrame."""
        return pl.read_database(query, connection=conn)

    def _get_batch_key_ranges(
        sql_server_source, batch_size: int, partition_key: str | None = None
    ) -> pl.DataFrame:
        """Gets INPT_FILE_KEY ranges from source table divided into batches."""
        start_date_str, end_date_str = __get_partition_range(partition_key)
        range_query = f"""
            WITH _ (INPT_FILE_KEY, ROW_NBR) AS (
                SELECT INPT_FILE_KEY
                      ,ROW_NUMBER() OVER(ORDER BY INPT_FILE_KEY)
                FROM irb.INPT_FILE
                WHERE (    UPDATED_DTTM >= '{start_date_str}'
                       AND UPDATED_DTTM <  '{end_date_str}')
                   OR NULLIF('{start_date_str}','') IS NULL
            )
            SELECT ((ROW_NBR-1)/{batch_size}) AS row_nbr
                  ,MIN(INPT_FILE_KEY) AS min_key
                  ,MAX(INPT_FILE_KEY) AS max_key
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
        """Fetch data from INPT_FILE for a given INPT_FILE_KEY range [min_key, max_key]."""
        query = f"""
            SELECT INPT_FILE_KEY
                  ,INPT_KEY
                  ,PRFL_DTL_KEY
                  ,ACCNT_TYPE
                  ,HLTH_PLN_ST
                  ,FUND_TYP
                  ,BG_SRC_CD
                  ,ORG_GRP_ID
                  ,GRP_NM
                  ,PBM_ACCNT_MNGR
                  ,ACCNT_EXEC
                  ,UNDRWRTR
                  ,CNTRCT_STRT_DT
                  ,CNTRCT_END_DT
                  ,CNTRCT_FNL_DT
                  ,LOCK_DT
                  ,RPRT_STRT_DT
                  ,RPRT_END_DT
                  ,PRIOR_YR_DAT_STRT_DT
                  ,PRIOR_YR_DAT_END_DT
                  ,ACTIVE_CLNT
                  ,ESI_SPCL_PRCNG
                  ,R90_IND
                  ,NTWRK_NUM
                  ,NTWRK_TYPE
                  ,OFFSETTING
                  ,RBT_OFFSET
                  ,SPCLTY_GRT_TYPE
                  ,ADJVNT
                  ,SSG_SPCLTY_EXCL
                  ,SPCLTY_DRG_MAIL_EXCL
                  ,SPCLTY_DRG_RTL_EXCL
                  ,BASE_ZBC_IND
                  ,ZBC_GRP
                  ,BASE_SSG_IND
                  ,SSG_MFG_CNT
                  ,AUTH_GNRC
                  ,BASE_CPD_IND
                  ,BASE_UNC_IND
                  ,BASE_MFN_IND
                  ,BRND_MAC
                  ,PAPER_CLMS
                  ,OON_CLMS
                  ,VAC_CLMS
                  ,SPLY_CLMS
                  ,EXCL_IH_NABP
                  ,NON_IH_NABP_EXCL
                  ,[340B_CLMS]
                  ,OTC_CLMS
                  ,MBR_PAY_DIFF
                  ,PWDR_CLMS
                  ,COB_CLMS
                  ,PTNT_LITG
                  ,SUBRGTN_CLMS
                  ,DRG_TYPE_CD_GNRC
                  ,DRG_TYPE_CD_EXCL
                  ,DAW
                  ,DAW_EXCL
                  ,PRVDR_EXCL
                  ,CLMS_THRSHLD
                  ,RTL_TIER_1
                  ,RB_GRT_1
                  ,RB_DF_GRT_1
                  ,RG_GRT_1
                  ,RG_DF_GRT_1
                  ,R_SPL_B_GRT_1
                  ,R_SPL_B_DF_GRT_1
                  ,R_SPL_G_GRT_1
                  ,R_SPL_G_DF_GRT_1
                  ,R_SPL_CMBD_GRT_1
                  ,R_SPL_CMBD_DF_GRT_1
                  ,R_SSG_GRT_1
                  ,R_SSG_DF_GRT_1
                  ,R90B_GRT_1
                  ,R90B_DF_GRT_1
                  ,R90G_GRT_1
                  ,R90G_DF_GRT_1
                  ,RTL_TIER_2
                  ,RB_GRT_2
                  ,RB_DF_GRT_2
                  ,RG_GRT_2
                  ,RG_DF_GRT_2
                  ,R_SPL_B_GRT_2
                  ,R_SPL_B_DF_GRT_2
                  ,R_SPL_G_GRT_2
                  ,R_SPL_G_DF_GRT_2
                  ,R_SPL_CMBD_GRT_2
                  ,R_SPL_CMBD_DF_GRT_2
                  ,R_SSG_GRT_2
                  ,R_SSG_DF_GRT_2
                  ,R90B_GRT_2
                  ,R90B_DF_GRT_2
                  ,R90G_GRT_2
                  ,R90G_DF_GRT_2
                  ,MAIL_DS_BRK_1
                  ,MB_GRT_1
                  ,MB_DF_GRT_1
                  ,MG_GRT_1
                  ,MG_DF_GRT_1
                  ,M_SPL_B_GRT_1
                  ,M_SPL_B_DF_GRT_1
                  ,M_SPL_G_GRT_1
                  ,M_SPL_G_DF_GRT_1
                  ,M_SPL_CMBD_GRT_1
                  ,M_SPL_CMBD_DF_GRT_1
                  ,M_SSG_GRT_1
                  ,M_SSG_DF_GRT_1
                  ,SPEC_AG_GRT
                  ,SPEC_AG_DF_GRT
                  ,SPEC_B_GRT
                  ,SPEC_B_DF_GRT
                  ,SPEC_G_GRT
                  ,SPEC_G_DF_GRT
                  ,SSG_GRT
                  ,SSG_DF_GRT
                  ,MAC2UNC_IND
                  ,NOTES
                  ,CUST_ID
                  ,SPCLTY_LIST
                  ,CVS_SPCLTY_CHNL_OVERRIDE
                  ,BRND_GEN_OVERRIDE
                  ,RTL_90
                  ,NTWRK_CHNNL_BR
                  ,CLMS_THRSHLD_TYPE
                  ,CAG_RMVL
                  ,SSG_LIST
                  ,PIC_ID
                  ,OUTLR_CLM
                  ,LDD_IND
                  ,NMD_IND
                  ,BIOSM_IND
                  ,SNPSHT_DT
                  ,NDC_EXCL
                  ,GPI_EXCL
                  ,INSLN_CLMS
                  ,GRRPS_PRCNG_IND
                  ,GRRPS_CNTRL_IND
                  ,SRX_LIST
                  ,R_GDR_GRT
                  ,M_GDR_GRT
                  ,CMBD_GDR_GRT
                  ,R_MAX_PNLTY
                  ,M_MAX_PNLTY
                  ,CMBD_MAX_PNLTY
                  ,INPT_FILE_STTS
                  ,INPT_FILE_NOTE
                  ,CREATED_BY
                  ,CREATED_DTTM
                  ,UPDATED_BY
                  ,UPDATED_DTTM
            FROM irb.INPT_FILE
            WHERE INPT_FILE_KEY BETWEEN {min_key} AND {max_key}
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
        source_ids = set(source_df["INPT_FILE_KEY"].to_list())
        target_ids = set(target_df["INPT_FILE_KEY"].to_list())
        insert_ids = list(source_ids - target_ids)
        potential_update_ids = list(source_ids & target_ids)
        delete_ids = list(target_ids - source_ids)

        if potential_update_ids:
            update_source_df = source_df.filter(
                pl.col("INPT_FILE_KEY").is_in(potential_update_ids)
            )
            update_target_df = target_df.filter(
                pl.col("INPT_FILE_KEY").is_in(potential_update_ids)
            )

            diff_condition = (
                __values_differ("INPT_KEY", "INPT_KEY_target")
                | __values_differ("PRFL_DTL_KEY", "PRFL_DTL_KEY_target")
                | __values_differ("ACCNT_TYPE", "ACCNT_TYPE_target")
                | __values_differ("HLTH_PLN_ST", "HLTH_PLN_ST_target")
                | __values_differ("FUND_TYP", "FUND_TYP_target")
                | __values_differ("BG_SRC_CD", "BG_SRC_CD_target")
                | __values_differ("ORG_GRP_ID", "ORG_GRP_ID_target")
                | __values_differ("GRP_NM", "GRP_NM_target")
                | __values_differ("PBM_ACCNT_MNGR", "PBM_ACCNT_MNGR_target")
                | __values_differ("ACCNT_EXEC", "ACCNT_EXEC_target")
                | __values_differ("UNDRWRTR", "UNDRWRTR_target")
                | __values_differ("CNTRCT_STRT_DT", "CNTRCT_STRT_DT_target")
                | __values_differ("CNTRCT_END_DT", "CNTRCT_END_DT_target")
                | __values_differ("CNTRCT_FNL_DT", "CNTRCT_FNL_DT_target")
                | __values_differ("LOCK_DT", "LOCK_DT_target")
                | __values_differ("RPRT_STRT_DT", "RPRT_STRT_DT_target")
                | __values_differ("RPRT_END_DT", "RPRT_END_DT_target")
                | __values_differ("PRIOR_YR_DAT_STRT_DT", "PRIOR_YR_DAT_STRT_DT_target")
                | __values_differ("PRIOR_YR_DAT_END_DT", "PRIOR_YR_DAT_END_DT_target")
                | __values_differ("ACTIVE_CLNT", "ACTIVE_CLNT_target")
                | __values_differ("ESI_SPCL_PRCNG", "ESI_SPCL_PRCNG_target")
                | __values_differ("R90_IND", "R90_IND_target")
                | __values_differ("NTWRK_NUM", "NTWRK_NUM_target")
                | __values_differ("NTWRK_TYPE", "NTWRK_TYPE_target")
                | __values_differ("OFFSETTING", "OFFSETTING_target")
                | __values_differ("RBT_OFFSET", "RBT_OFFSET_target")
                | __values_differ("SPCLTY_GRT_TYPE", "SPCLTY_GRT_TYPE_target")
                | __values_differ("ADJVNT", "ADJVNT_target")
                | __values_differ("SSG_SPCLTY_EXCL", "SSG_SPCLTY_EXCL_target")
                | __values_differ("SPCLTY_DRG_MAIL_EXCL", "SPCLTY_DRG_MAIL_EXCL_target")
                | __values_differ("SPCLTY_DRG_RTL_EXCL", "SPCLTY_DRG_RTL_EXCL_target")
                | __values_differ("BASE_ZBC_IND", "BASE_ZBC_IND_target")
                | __values_differ("ZBC_GRP", "ZBC_GRP_target")
                | __values_differ("BASE_SSG_IND", "BASE_SSG_IND_target")
                | __values_differ("SSG_MFG_CNT", "SSG_MFG_CNT_target")
                | __values_differ("AUTH_GNRC", "AUTH_GNRC_target")
                | __values_differ("BASE_CPD_IND", "BASE_CPD_IND_target")
                | __values_differ("BASE_UNC_IND", "BASE_UNC_IND_target")
                | __values_differ("BASE_MFN_IND", "BASE_MFN_IND_target")
                | __values_differ("BRND_MAC", "BRND_MAC_target")
                | __values_differ("PAPER_CLMS", "PAPER_CLMS_target")
                | __values_differ("OON_CLMS", "OON_CLMS_target")
                | __values_differ("VAC_CLMS", "VAC_CLMS_target")
                | __values_differ("SPLY_CLMS", "SPLY_CLMS_target")
                | __values_differ("EXCL_IH_NABP", "EXCL_IH_NABP_target")
                | __values_differ("NON_IH_NABP_EXCL", "NON_IH_NABP_EXCL_target")
                | __values_differ("340B_CLMS", "340B_CLMS_target")
                | __values_differ("OTC_CLMS", "OTC_CLMS_target")
                | __values_differ("MBR_PAY_DIFF", "MBR_PAY_DIFF_target")
                | __values_differ("PWDR_CLMS", "PWDR_CLMS_target")
                | __values_differ("COB_CLMS", "COB_CLMS_target")
                | __values_differ("PTNT_LITG", "PTNT_LITG_target")
                | __values_differ("SUBRGTN_CLMS", "SUBRGTN_CLMS_target")
                | __values_differ("DRG_TYPE_CD_GNRC", "DRG_TYPE_CD_GNRC_target")
                | __values_differ("DRG_TYPE_CD_EXCL", "DRG_TYPE_CD_EXCL_target")
                | __values_differ("DAW", "DAW_target")
                | __values_differ("DAW_EXCL", "DAW_EXCL_target")
                | __values_differ("PRVDR_EXCL", "PRVDR_EXCL_target")
                | __values_differ("CLMS_THRSHLD", "CLMS_THRSHLD_target")
                | __values_differ("RTL_TIER_1", "RTL_TIER_1_target")
                | __values_differ("RB_GRT_1", "RB_GRT_1_target")
                | __values_differ("RB_DF_GRT_1", "RB_DF_GRT_1_target")
                | __values_differ("RG_GRT_1", "RG_GRT_1_target")
                | __values_differ("RG_DF_GRT_1", "RG_DF_GRT_1_target")
                | __values_differ("R_SPL_B_GRT_1", "R_SPL_B_GRT_1_target")
                | __values_differ("R_SPL_B_DF_GRT_1", "R_SPL_B_DF_GRT_1_target")
                | __values_differ("R_SPL_G_GRT_1", "R_SPL_G_GRT_1_target")
                | __values_differ("R_SPL_G_DF_GRT_1", "R_SPL_G_DF_GRT_1_target")
                | __values_differ("R_SPL_CMBD_GRT_1", "R_SPL_CMBD_GRT_1_target")
                | __values_differ("R_SPL_CMBD_DF_GRT_1", "R_SPL_CMBD_DF_GRT_1_target")
                | __values_differ("R_SSG_GRT_1", "R_SSG_GRT_1_target")
                | __values_differ("R_SSG_DF_GRT_1", "R_SSG_DF_GRT_1_target")
                | __values_differ("R90B_GRT_1", "R90B_GRT_1_target")
                | __values_differ("R90B_DF_GRT_1", "R90B_DF_GRT_1_target")
                | __values_differ("R90G_GRT_1", "R90G_GRT_1_target")
                | __values_differ("R90G_DF_GRT_1", "R90G_DF_GRT_1_target")
                | __values_differ("RTL_TIER_2", "RTL_TIER_2_target")
                | __values_differ("RB_GRT_2", "RB_GRT_2_target")
                | __values_differ("RB_DF_GRT_2", "RB_DF_GRT_2_target")
                | __values_differ("RG_GRT_2", "RG_GRT_2_target")
                | __values_differ("RG_DF_GRT_2", "RG_DF_GRT_2_target")
                | __values_differ("R_SPL_B_GRT_2", "R_SPL_B_GRT_2_target")
                | __values_differ("R_SPL_B_DF_GRT_2", "R_SPL_B_DF_GRT_2_target")
                | __values_differ("R_SPL_G_GRT_2", "R_SPL_G_GRT_2_target")
                | __values_differ("R_SPL_G_DF_GRT_2", "R_SPL_G_DF_GRT_2_target")
                | __values_differ("R_SPL_CMBD_GRT_2", "R_SPL_CMBD_GRT_2_target")
                | __values_differ("R_SPL_CMBD_DF_GRT_2", "R_SPL_CMBD_DF_GRT_2_target")
                | __values_differ("R_SSG_GRT_2", "R_SSG_GRT_2_target")
                | __values_differ("R_SSG_DF_GRT_2", "R_SSG_DF_GRT_2_target")
                | __values_differ("R90B_GRT_2", "R90B_GRT_2_target")
                | __values_differ("R90B_DF_GRT_2", "R90B_DF_GRT_2_target")
                | __values_differ("R90G_GRT_2", "R90G_GRT_2_target")
                | __values_differ("R90G_DF_GRT_2", "R90G_DF_GRT_2_target")
                | __values_differ("MAIL_DS_BRK_1", "MAIL_DS_BRK_1_target")
                | __values_differ("MB_GRT_1", "MB_GRT_1_target")
                | __values_differ("MB_DF_GRT_1", "MB_DF_GRT_1_target")
                | __values_differ("MG_GRT_1", "MG_GRT_1_target")
                | __values_differ("MG_DF_GRT_1", "MG_DF_GRT_1_target")
                | __values_differ("M_SPL_B_GRT_1", "M_SPL_B_GRT_1_target")
                | __values_differ("M_SPL_B_DF_GRT_1", "M_SPL_B_DF_GRT_1_target")
                | __values_differ("M_SPL_G_GRT_1", "M_SPL_G_GRT_1_target")
                | __values_differ("M_SPL_G_DF_GRT_1", "M_SPL_G_DF_GRT_1_target")
                | __values_differ("M_SPL_CMBD_GRT_1", "M_SPL_CMBD_GRT_1_target")
                | __values_differ("M_SPL_CMBD_DF_GRT_1", "M_SPL_CMBD_DF_GRT_1_target")
                | __values_differ("M_SSG_GRT_1", "M_SSG_GRT_1_target")
                | __values_differ("M_SSG_DF_GRT_1", "M_SSG_DF_GRT_1_target")
                | __values_differ("SPEC_AG_GRT", "SPEC_AG_GRT_target")
                | __values_differ("SPEC_AG_DF_GRT", "SPEC_AG_DF_GRT_target")
                | __values_differ("SPEC_B_GRT", "SPEC_B_GRT_target")
                | __values_differ("SPEC_B_DF_GRT", "SPEC_B_DF_GRT_target")
                | __values_differ("SPEC_G_GRT", "SPEC_G_GRT_target")
                | __values_differ("SPEC_G_DF_GRT", "SPEC_G_DF_GRT_target")
                | __values_differ("SSG_GRT", "SSG_GRT_target")
                | __values_differ("SSG_DF_GRT", "SSG_DF_GRT_target")
                | __values_differ("MAC2UNC_IND", "MAC2UNC_IND_target")
                | __values_differ("NOTES", "NOTES_target")
                | __values_differ("CUST_ID", "CUST_ID_target")
                | __values_differ("SPCLTY_LIST", "SPCLTY_LIST_target")
                | __values_differ("CVS_SPCLTY_CHNL_OVERRIDE", "CVS_SPCLTY_CHNL_OVERRIDE_target")
                | __values_differ("BRND_GEN_OVERRIDE", "BRND_GEN_OVERRIDE_target")
                | __values_differ("RTL_90", "RTL_90_target")
                | __values_differ("NTWRK_CHNNL_BR", "NTWRK_CHNNL_BR_target")
                | __values_differ("CLMS_THRSHLD_TYPE", "CLMS_THRSHLD_TYPE_target")
                | __values_differ("CAG_RMVL", "CAG_RMVL_target")
                | __values_differ("SSG_LIST", "SSG_LIST_target")
                | __values_differ("PIC_ID", "PIC_ID_target")
                | __values_differ("OUTLR_CLM", "OUTLR_CLM_target")
                | __values_differ("LDD_IND", "LDD_IND_target")
                | __values_differ("NMD_IND", "NMD_IND_target")
                | __values_differ("BIOSM_IND", "BIOSM_IND_target")
                | __values_differ("SNPSHT_DT", "SNPSHT_DT_target")
                | __values_differ("NDC_EXCL", "NDC_EXCL_target")
                | __values_differ("GPI_EXCL", "GPI_EXCL_target")
                | __values_differ("INSLN_CLMS", "INSLN_CLMS_target")
                | __values_differ("GRRPS_PRCNG_IND", "GRRPS_PRCNG_IND_target")
                | __values_differ("GRRPS_CNTRL_IND", "GRRPS_CNTRL_IND_target")
                | __values_differ("SRX_LIST", "SRX_LIST_target")
                | __values_differ("R_GDR_GRT", "R_GDR_GRT_target")
                | __values_differ("M_GDR_GRT", "M_GDR_GRT_target")
                | __values_differ("CMBD_GDR_GRT", "CMBD_GDR_GRT_target")
                | __values_differ("R_MAX_PNLTY", "R_MAX_PNLTY_target")
                | __values_differ("M_MAX_PNLTY", "M_MAX_PNLTY_target")
                | __values_differ("CMBD_MAX_PNLTY", "CMBD_MAX_PNLTY_target")
                | __values_differ("INPT_FILE_STTS", "INPT_FILE_STTS_target")
                | __values_differ("INPT_FILE_NOTE", "INPT_FILE_NOTE_target")
                | __values_differ("CREATED_BY", "CREATED_BY_target")
                | __values_differ("CREATED_DTTM", "CREATED_DTTM_target")
                | __values_differ("UPDATED_BY", "UPDATED_BY_target")
                | __values_differ("UPDATED_DTTM", "UPDATED_DTTM_target")
            )

            diff_df = update_source_df.join(
                update_target_df, on="INPT_FILE_KEY", how="left", suffix="_target"
            ).filter(diff_condition)

            update_ids = diff_df["INPT_FILE_KEY"].to_list()
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
        insert_data_df = source_df.filter(pl.col("INPT_FILE_KEY").is_in(insert_ids))
        insert_data_pl = _num_to_str(insert_data_df)
        insert_data_np = insert_data_pl.to_numpy()
        insert_data_ls = insert_data_np.tolist()

        with sql_server_target() as conn_target:
            with conn_target.cursor() as cursor_target:
                cursor_target.execute(
                    "ALTER TABLE irb.INPT_FILE DISABLE TRIGGER ALL"
                )                
                cursor_target.execute(
                    "SET IDENTITY_INSERT irb.INPT_FILE ON"
                )                
                cursor_target.fast_executemany = True
                cursor_target.executemany(
                    """
                    INSERT INTO irb.INPT_FILE 
                      (
                        INPT_FILE_KEY
                       ,INPT_KEY
                       ,PRFL_DTL_KEY
                       ,ACCNT_TYPE
                       ,HLTH_PLN_ST
                       ,FUND_TYP
                       ,BG_SRC_CD
                       ,ORG_GRP_ID
                       ,GRP_NM
                       ,PBM_ACCNT_MNGR
                       ,ACCNT_EXEC
                       ,UNDRWRTR
                       ,CNTRCT_STRT_DT
                       ,CNTRCT_END_DT
                       ,CNTRCT_FNL_DT
                       ,LOCK_DT
                       ,RPRT_STRT_DT
                       ,RPRT_END_DT
                       ,PRIOR_YR_DAT_STRT_DT
                       ,PRIOR_YR_DAT_END_DT
                       ,ACTIVE_CLNT
                       ,ESI_SPCL_PRCNG
                       ,R90_IND
                       ,NTWRK_NUM
                       ,NTWRK_TYPE
                       ,OFFSETTING
                       ,RBT_OFFSET
                       ,SPCLTY_GRT_TYPE
                       ,ADJVNT
                       ,SSG_SPCLTY_EXCL
                       ,SPCLTY_DRG_MAIL_EXCL
                       ,SPCLTY_DRG_RTL_EXCL
                       ,BASE_ZBC_IND
                       ,ZBC_GRP
                       ,BASE_SSG_IND
                       ,SSG_MFG_CNT
                       ,AUTH_GNRC
                       ,BASE_CPD_IND
                       ,BASE_UNC_IND
                       ,BASE_MFN_IND
                       ,BRND_MAC
                       ,PAPER_CLMS
                       ,OON_CLMS
                       ,VAC_CLMS
                       ,SPLY_CLMS
                       ,EXCL_IH_NABP
                       ,NON_IH_NABP_EXCL
                       ,[340B_CLMS]
                       ,OTC_CLMS
                       ,MBR_PAY_DIFF
                       ,PWDR_CLMS
                       ,COB_CLMS
                       ,PTNT_LITG
                       ,SUBRGTN_CLMS
                       ,DRG_TYPE_CD_GNRC
                       ,DRG_TYPE_CD_EXCL
                       ,DAW
                       ,DAW_EXCL
                       ,PRVDR_EXCL
                       ,CLMS_THRSHLD
                       ,RTL_TIER_1
                       ,RB_GRT_1
                       ,RB_DF_GRT_1
                       ,RG_GRT_1
                       ,RG_DF_GRT_1
                       ,R_SPL_B_GRT_1
                       ,R_SPL_B_DF_GRT_1
                       ,R_SPL_G_GRT_1
                       ,R_SPL_G_DF_GRT_1
                       ,R_SPL_CMBD_GRT_1
                       ,R_SPL_CMBD_DF_GRT_1
                       ,R_SSG_GRT_1
                       ,R_SSG_DF_GRT_1
                       ,R90B_GRT_1
                       ,R90B_DF_GRT_1
                       ,R90G_GRT_1
                       ,R90G_DF_GRT_1
                       ,RTL_TIER_2
                       ,RB_GRT_2
                       ,RB_DF_GRT_2
                       ,RG_GRT_2
                       ,RG_DF_GRT_2
                       ,R_SPL_B_GRT_2
                       ,R_SPL_B_DF_GRT_2
                       ,R_SPL_G_GRT_2
                       ,R_SPL_G_DF_GRT_2
                       ,R_SPL_CMBD_GRT_2
                       ,R_SPL_CMBD_DF_GRT_2
                       ,R_SSG_GRT_2
                       ,R_SSG_DF_GRT_2
                       ,R90B_GRT_2
                       ,R90B_DF_GRT_2
                       ,R90G_GRT_2
                       ,R90G_DF_GRT_2
                       ,MAIL_DS_BRK_1
                       ,MB_GRT_1
                       ,MB_DF_GRT_1
                       ,MG_GRT_1
                       ,MG_DF_GRT_1
                       ,M_SPL_B_GRT_1
                       ,M_SPL_B_DF_GRT_1
                       ,M_SPL_G_GRT_1
                       ,M_SPL_G_DF_GRT_1
                       ,M_SPL_CMBD_GRT_1
                       ,M_SPL_CMBD_DF_GRT_1
                       ,M_SSG_GRT_1
                       ,M_SSG_DF_GRT_1
                       ,SPEC_AG_GRT
                       ,SPEC_AG_DF_GRT
                       ,SPEC_B_GRT
                       ,SPEC_B_DF_GRT
                       ,SPEC_G_GRT
                       ,SPEC_G_DF_GRT
                       ,SSG_GRT
                       ,SSG_DF_GRT
                       ,MAC2UNC_IND
                       ,NOTES
                       ,CUST_ID
                       ,SPCLTY_LIST
                       ,CVS_SPCLTY_CHNL_OVERRIDE
                       ,BRND_GEN_OVERRIDE
                       ,RTL_90
                       ,NTWRK_CHNNL_BR
                       ,CLMS_THRSHLD_TYPE
                       ,CAG_RMVL
                       ,SSG_LIST
                       ,PIC_ID
                       ,OUTLR_CLM
                       ,LDD_IND
                       ,NMD_IND
                       ,BIOSM_IND
                       ,SNPSHT_DT
                       ,NDC_EXCL
                       ,GPI_EXCL
                       ,INSLN_CLMS
                       ,GRRPS_PRCNG_IND
                       ,GRRPS_CNTRL_IND
                       ,SRX_LIST
                       ,R_GDR_GRT
                       ,M_GDR_GRT
                       ,CMBD_GDR_GRT
                       ,R_MAX_PNLTY
                       ,M_MAX_PNLTY
                       ,CMBD_MAX_PNLTY
                       ,INPT_FILE_STTS
                       ,INPT_FILE_NOTE
                       ,CREATED_BY
                       ,CREATED_DTTM
                       ,UPDATED_BY
                       ,UPDATED_DTTM
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_data_ls,
                )
                cursor_target.execute(
                    "SET IDENTITY_INSERT irb.INPT_FILE OFF"
                )                
                cursor_target.execute(
                    "ALTER TABLE irb.INPT_FILE ENABLE TRIGGER ALL"
                )
                conn_target.commit()

        return len(insert_ids)

    def _update_batch_data(source_df: pl.DataFrame, update_ids: list) -> int:
        """Update existing records in the target table."""
        update_data = source_df.filter(pl.col("INPT_FILE_KEY").is_in(update_ids))
        update_data_df = update_data.select(
            [
                pl.col("INPT_KEY"),
                pl.col("PRFL_DTL_KEY"),
                pl.col("ACCNT_TYPE"),
                pl.col("HLTH_PLN_ST"),
                pl.col("FUND_TYP"),
                pl.col("BG_SRC_CD"),
                pl.col("ORG_GRP_ID"),
                pl.col("GRP_NM"),
                pl.col("PBM_ACCNT_MNGR"),
                pl.col("ACCNT_EXEC"),
                pl.col("UNDRWRTR"),
                pl.col("CNTRCT_STRT_DT"),
                pl.col("CNTRCT_END_DT"),
                pl.col("CNTRCT_FNL_DT"),
                pl.col("LOCK_DT"),
                pl.col("RPRT_STRT_DT"),
                pl.col("RPRT_END_DT"),
                pl.col("PRIOR_YR_DAT_STRT_DT"),
                pl.col("PRIOR_YR_DAT_END_DT"),
                pl.col("ACTIVE_CLNT"),
                pl.col("ESI_SPCL_PRCNG"),
                pl.col("R90_IND"),
                pl.col("NTWRK_NUM"),
                pl.col("NTWRK_TYPE"),
                pl.col("OFFSETTING"),
                pl.col("RBT_OFFSET"),
                pl.col("SPCLTY_GRT_TYPE"),
                pl.col("ADJVNT"),
                pl.col("SSG_SPCLTY_EXCL"),
                pl.col("SPCLTY_DRG_MAIL_EXCL"),
                pl.col("SPCLTY_DRG_RTL_EXCL"),
                pl.col("BASE_ZBC_IND"),
                pl.col("ZBC_GRP"),
                pl.col("BASE_SSG_IND"),
                pl.col("SSG_MFG_CNT"),
                pl.col("AUTH_GNRC"),
                pl.col("BASE_CPD_IND"),
                pl.col("BASE_UNC_IND"),
                pl.col("BASE_MFN_IND"),
                pl.col("BRND_MAC"),
                pl.col("PAPER_CLMS"),
                pl.col("OON_CLMS"),
                pl.col("VAC_CLMS"),
                pl.col("SPLY_CLMS"),
                pl.col("EXCL_IH_NABP"),
                pl.col("NON_IH_NABP_EXCL"),
                pl.col("340B_CLMS"),
                pl.col("OTC_CLMS"),
                pl.col("MBR_PAY_DIFF"),
                pl.col("PWDR_CLMS"),
                pl.col("COB_CLMS"),
                pl.col("PTNT_LITG"),
                pl.col("SUBRGTN_CLMS"),
                pl.col("DRG_TYPE_CD_GNRC"),
                pl.col("DRG_TYPE_CD_EXCL"),
                pl.col("DAW"),
                pl.col("DAW_EXCL"),
                pl.col("PRVDR_EXCL"),
                pl.col("CLMS_THRSHLD"),
                pl.col("RTL_TIER_1"),
                pl.col("RB_GRT_1"),
                pl.col("RB_DF_GRT_1"),
                pl.col("RG_GRT_1"),
                pl.col("RG_DF_GRT_1"),
                pl.col("R_SPL_B_GRT_1"),
                pl.col("R_SPL_B_DF_GRT_1"),
                pl.col("R_SPL_G_GRT_1"),
                pl.col("R_SPL_G_DF_GRT_1"),
                pl.col("R_SPL_CMBD_GRT_1"),
                pl.col("R_SPL_CMBD_DF_GRT_1"),
                pl.col("R_SSG_GRT_1"),
                pl.col("R_SSG_DF_GRT_1"),
                pl.col("R90B_GRT_1"),
                pl.col("R90B_DF_GRT_1"),
                pl.col("R90G_GRT_1"),
                pl.col("R90G_DF_GRT_1"),
                pl.col("RTL_TIER_2"),
                pl.col("RB_GRT_2"),
                pl.col("RB_DF_GRT_2"),
                pl.col("RG_GRT_2"),
                pl.col("RG_DF_GRT_2"),
                pl.col("R_SPL_B_GRT_2"),
                pl.col("R_SPL_B_DF_GRT_2"),
                pl.col("R_SPL_G_GRT_2"),
                pl.col("R_SPL_G_DF_GRT_2"),
                pl.col("R_SPL_CMBD_GRT_2"),
                pl.col("R_SPL_CMBD_DF_GRT_2"),
                pl.col("R_SSG_GRT_2"),
                pl.col("R_SSG_DF_GRT_2"),
                pl.col("R90B_GRT_2"),
                pl.col("R90B_DF_GRT_2"),
                pl.col("R90G_GRT_2"),
                pl.col("R90G_DF_GRT_2"),
                pl.col("MAIL_DS_BRK_1"),
                pl.col("MB_GRT_1"),
                pl.col("MB_DF_GRT_1"),
                pl.col("MG_GRT_1"),
                pl.col("MG_DF_GRT_1"),
                pl.col("M_SPL_B_GRT_1"),
                pl.col("M_SPL_B_DF_GRT_1"),
                pl.col("M_SPL_G_GRT_1"),
                pl.col("M_SPL_G_DF_GRT_1"),
                pl.col("M_SPL_CMBD_GRT_1"),
                pl.col("M_SPL_CMBD_DF_GRT_1"),
                pl.col("M_SSG_GRT_1"),
                pl.col("M_SSG_DF_GRT_1"),
                pl.col("SPEC_AG_GRT"),
                pl.col("SPEC_AG_DF_GRT"),
                pl.col("SPEC_B_GRT"),
                pl.col("SPEC_B_DF_GRT"),
                pl.col("SPEC_G_GRT"),
                pl.col("SPEC_G_DF_GRT"),
                pl.col("SSG_GRT"),
                pl.col("SSG_DF_GRT"),
                pl.col("MAC2UNC_IND"),
                pl.col("NOTES"),
                pl.col("CUST_ID"),
                pl.col("SPCLTY_LIST"),
                pl.col("CVS_SPCLTY_CHNL_OVERRIDE"),
                pl.col("BRND_GEN_OVERRIDE"),
                pl.col("RTL_90"),
                pl.col("NTWRK_CHNNL_BR"),
                pl.col("CLMS_THRSHLD_TYPE"),
                pl.col("CAG_RMVL"),
                pl.col("SSG_LIST"),
                pl.col("PIC_ID"),
                pl.col("OUTLR_CLM"),
                pl.col("LDD_IND"),
                pl.col("NMD_IND"),
                pl.col("BIOSM_IND"),
                pl.col("SNPSHT_DT"),
                pl.col("NDC_EXCL"),
                pl.col("GPI_EXCL"),
                pl.col("INSLN_CLMS"),
                pl.col("GRRPS_PRCNG_IND"),
                pl.col("GRRPS_CNTRL_IND"),
                pl.col("SRX_LIST"),
                pl.col("R_GDR_GRT"),
                pl.col("M_GDR_GRT"),
                pl.col("CMBD_GDR_GRT"),
                pl.col("R_MAX_PNLTY"),
                pl.col("M_MAX_PNLTY"),
                pl.col("CMBD_MAX_PNLTY"),
                pl.col("INPT_FILE_STTS"),
                pl.col("INPT_FILE_NOTE"),
                pl.col("CREATED_BY"),
                pl.col("CREATED_DTTM"),
                pl.col("UPDATED_BY"),
                pl.col("UPDATED_DTTM"),
                pl.col("INPT_FILE_KEY"),
            ]
        )
        update_data_pl = _num_to_str(update_data_df)
        update_data_np = update_data_pl.to_numpy()
        update_data_ls = update_data_np.tolist()

        with sql_server_target() as conn_target:
            with conn_target.cursor() as cursor_target:
                cursor_target.execute(
                    "ALTER TABLE irb.INPT_FILE DISABLE TRIGGER ALL"
                )
                cursor_target.executemany(
                    """
                    UPDATE irb.INPT_FILE
                    SET INPT_KEY = ?
                       ,PRFL_DTL_KEY = ?
                       ,ACCNT_TYPE = ?
                       ,HLTH_PLN_ST = ?
                       ,FUND_TYP = ?
                       ,BG_SRC_CD = ?
                       ,ORG_GRP_ID = ?
                       ,GRP_NM = ?
                       ,PBM_ACCNT_MNGR = ?
                       ,ACCNT_EXEC = ?
                       ,UNDRWRTR = ?
                       ,CNTRCT_STRT_DT = ?
                       ,CNTRCT_END_DT = ?
                       ,CNTRCT_FNL_DT = ?
                       ,LOCK_DT = ?
                       ,RPRT_STRT_DT = ?
                       ,RPRT_END_DT = ?
                       ,PRIOR_YR_DAT_STRT_DT = ?
                       ,PRIOR_YR_DAT_END_DT = ?
                       ,ACTIVE_CLNT = ?
                       ,ESI_SPCL_PRCNG = ?
                       ,R90_IND = ?
                       ,NTWRK_NUM = ?
                       ,NTWRK_TYPE = ?
                       ,OFFSETTING = ?
                       ,RBT_OFFSET = ?
                       ,SPCLTY_GRT_TYPE = ?
                       ,ADJVNT = ?
                       ,SSG_SPCLTY_EXCL = ?
                       ,SPCLTY_DRG_MAIL_EXCL = ?
                       ,SPCLTY_DRG_RTL_EXCL = ?
                       ,BASE_ZBC_IND = ?
                       ,ZBC_GRP = ?
                       ,BASE_SSG_IND = ?
                       ,SSG_MFG_CNT = ?
                       ,AUTH_GNRC = ?
                       ,BASE_CPD_IND = ?
                       ,BASE_UNC_IND = ?
                       ,BASE_MFN_IND = ?
                       ,BRND_MAC = ?
                       ,PAPER_CLMS = ?
                       ,OON_CLMS = ?
                       ,VAC_CLMS = ?
                       ,SPLY_CLMS = ?
                       ,EXCL_IH_NABP = ?
                       ,NON_IH_NABP_EXCL = ?
                       ,[340B_CLMS] = ?
                       ,OTC_CLMS = ?
                       ,MBR_PAY_DIFF = ?
                       ,PWDR_CLMS = ?
                       ,COB_CLMS = ?
                       ,PTNT_LITG = ?
                       ,SUBRGTN_CLMS = ?
                       ,DRG_TYPE_CD_GNRC = ?
                       ,DRG_TYPE_CD_EXCL = ?
                       ,DAW = ?
                       ,DAW_EXCL = ?
                       ,PRVDR_EXCL = ?
                       ,CLMS_THRSHLD = ?
                       ,RTL_TIER_1 = ?
                       ,RB_GRT_1 = ?
                       ,RB_DF_GRT_1 = ?
                       ,RG_GRT_1 = ?
                       ,RG_DF_GRT_1 = ?
                       ,R_SPL_B_GRT_1 = ?
                       ,R_SPL_B_DF_GRT_1 = ?
                       ,R_SPL_G_GRT_1 = ?
                       ,R_SPL_G_DF_GRT_1 = ?
                       ,R_SPL_CMBD_GRT_1 = ?
                       ,R_SPL_CMBD_DF_GRT_1 = ?
                       ,R_SSG_GRT_1 = ?
                       ,R_SSG_DF_GRT_1 = ?
                       ,R90B_GRT_1 = ?
                       ,R90B_DF_GRT_1 = ?
                       ,R90G_GRT_1 = ?
                       ,R90G_DF_GRT_1 = ?
                       ,RTL_TIER_2 = ?
                       ,RB_GRT_2 = ?
                       ,RB_DF_GRT_2 = ?
                       ,RG_GRT_2 = ?
                       ,RG_DF_GRT_2 = ?
                       ,R_SPL_B_GRT_2 = ?
                       ,R_SPL_B_DF_GRT_2 = ?
                       ,R_SPL_G_GRT_2 = ?
                       ,R_SPL_G_DF_GRT_2 = ?
                       ,R_SPL_CMBD_GRT_2 = ?
                       ,R_SPL_CMBD_DF_GRT_2 = ?
                       ,R_SSG_GRT_2 = ?
                       ,R_SSG_DF_GRT_2 = ?
                       ,R90B_GRT_2 = ?
                       ,R90B_DF_GRT_2 = ?
                       ,R90G_GRT_2 = ?
                       ,R90G_DF_GRT_2 = ?
                       ,MAIL_DS_BRK_1 = ?
                       ,MB_GRT_1 = ?
                       ,MB_DF_GRT_1 = ?
                       ,MG_GRT_1 = ?
                       ,MG_DF_GRT_1 = ?
                       ,M_SPL_B_GRT_1 = ?
                       ,M_SPL_B_DF_GRT_1 = ?
                       ,M_SPL_G_GRT_1 = ?
                       ,M_SPL_G_DF_GRT_1 = ?
                       ,M_SPL_CMBD_GRT_1 = ?
                       ,M_SPL_CMBD_DF_GRT_1 = ?
                       ,M_SSG_GRT_1 = ?
                       ,M_SSG_DF_GRT_1 = ?
                       ,SPEC_AG_GRT = ?
                       ,SPEC_AG_DF_GRT = ?
                       ,SPEC_B_GRT = ?
                       ,SPEC_B_DF_GRT = ?
                       ,SPEC_G_GRT = ?
                       ,SPEC_G_DF_GRT = ?
                       ,SSG_GRT = ?
                       ,SSG_DF_GRT = ?
                       ,MAC2UNC_IND = ?
                       ,NOTES = ?
                       ,CUST_ID = ?
                       ,SPCLTY_LIST = ?
                       ,CVS_SPCLTY_CHNL_OVERRIDE = ?
                       ,BRND_GEN_OVERRIDE = ?
                       ,RTL_90 = ?
                       ,NTWRK_CHNNL_BR = ?
                       ,CLMS_THRSHLD_TYPE = ?
                       ,CAG_RMVL = ?
                       ,SSG_LIST = ?
                       ,PIC_ID = ?
                       ,OUTLR_CLM = ?
                       ,LDD_IND = ?
                       ,NMD_IND = ?
                       ,BIOSM_IND = ?
                       ,SNPSHT_DT = ?
                       ,NDC_EXCL = ?
                       ,GPI_EXCL = ?
                       ,INSLN_CLMS = ?
                       ,GRRPS_PRCNG_IND = ?
                       ,GRRPS_CNTRL_IND = ?
                       ,SRX_LIST = ?
                       ,R_GDR_GRT = ?
                       ,M_GDR_GRT = ?
                       ,CMBD_GDR_GRT = ?
                       ,R_MAX_PNLTY = ?
                       ,M_MAX_PNLTY = ?
                       ,CMBD_MAX_PNLTY = ?
                       ,INPT_FILE_STTS = ?
                       ,INPT_FILE_NOTE = ?
                       ,CREATED_BY = ?
                       ,CREATED_DTTM = ?
                       ,UPDATED_BY = ?
                       ,UPDATED_DTTM = ?
                    WHERE INPT_FILE_KEY = ?
                    """,
                    update_data_ls,
                )
                cursor_target.execute(
                    "ALTER TABLE irb.INPT_FILE ENABLE TRIGGER ALL"
                )
                conn_target.commit()

        return len(update_ids)

    # Step 0: initialize
    start_datetime, start_time = str(datetime.now().astimezone()), time.time()
    sql_server_source = context.resources.sql_server_source
    sql_server_target = context.resources.sql_server_target
    batch_size = int(constants.BATCH_SIZE)
    inserts = updates = deletes = ignores = min_key = max_key = row_count = seconds = (
        records_per_second
    ) = batch_count = 0

    # Step 1: get batch interval key ranges from source and target
    with ThreadPoolExecutor(max_workers=2) as pool:
        src_future = pool.submit(
            _get_batch_key_ranges, sql_server_source, batch_size, None
        )
        tgt_future = pool.submit(
            _get_batch_key_ranges, sql_server_target, batch_size, None
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

        if len(delete_ids) > 0:
            context.log.error(
                f"Batch {batch['row_nbr']+1:,} of {max_batch}: "
                f"INPT_FILE_KEY {batch_min_key:} to {batch_max_key:} => "
                f"{len(delete_ids):,} deleted records detected"
            )
            raise dg.Failure("Deletes detected in source data; manual intervention required.")
        else:
            # Steps 5/6/7: perform DML operations
            batch_inserts = _insert_batch_data(source_df, insert_ids) if insert_ids else 0
            batch_updates = _update_batch_data(source_df, update_ids) if update_ids else 0
            batch_deletes = 0
            batch_ignores = len(ignore_ids)

            # Step 8: echo status to console and update accumulators
            context.log.info(
                f"Batch {batch['row_nbr']+1:,} of {max_batch}: "
                f"INPT_FILE_KEY {batch_min_key:} to {batch_max_key:} => "
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
    batch_count = merged_ranges.height
    min_key = int(merged_ranges.select("min_key").min().item()) if batch_count > 0 else 0
    max_key = int(merged_ranges.select("max_key").max().item()) if batch_count > 0 else 0
    row_count = inserts + updates + deletes + ignores
    user_name, host_name = getpass.getuser(), socket.gethostname()
    project_version = tomllib.load(Path('pyproject.toml').open('rb'))['project']['version']
    end_datetime, end_time = str(datetime.now().astimezone()), time.time()
    seconds = round(end_time - start_time, 3)
    records_per_second = int(round(row_count / seconds if seconds > 0 else 0, 0))

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
            "USERNM": dg.MetadataValue.text(user_name),
            "HOSTNM": dg.MetadataValue.text(host_name),
            "DGVRSN": dg.MetadataValue.text(project_version),
            "STRDTM": dg.MetadataValue.text(start_datetime),
            "ENDDTM": dg.MetadataValue.text(end_datetime),
            "SECNDS": dg.MetadataValue.float(seconds),
            "RCRD_S": dg.MetadataValue.int(records_per_second),
        }
    )
