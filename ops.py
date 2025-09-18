# src/pic_automation_dev/defs/ops.py
import dagster as dg


@dg.asset(
    group_name="wps_clnt_grt",
    key=dg.AssetKey(["localdb", "disable_fks"]),
    kinds={"sqlserver"},
    required_resource_keys={"sql_server_target"},
)
def disable_fks(context: dg.AssetExecutionContext):
    with context.resources.sql_server_target() as conn, conn.cursor() as cur:
        context.log.info("Disabling FK__CUST__FMAC_PIC_KEY_MSTR and FK__PIC__UPLD_KEY_MSTR")
        cur.execute("ALTER TABLE irb.CUST NOCHECK CONSTRAINT FK__CUST__FMAC_PIC_KEY_MSTR")
        cur.execute("ALTER TABLE irb.PIC NOCHECK CONSTRAINT FK__PIC__UPLD_KEY_MSTR")
        conn.commit()

@dg.asset(
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    deps=["fmac_pic", "upld"],
    group_name="wps_clnt_grt",
    key=dg.AssetKey(["localdb", "enable_fks"]),
    kinds={"sqlserver"},
    required_resource_keys={"sql_server_target"},
)
def enable_fks(context: dg.AssetExecutionContext):
    with context.resources.sql_server_target() as conn, conn.cursor() as cur:
        context.log.info("Enabling FK__CUST__FMAC_PIC_KEY_MSTR and FK__PIC__UPLD_KEY_MSTR")
        cur.execute("ALTER TABLE irb.CUST WITH CHECK CHECK CONSTRAINT FK__CUST__FMAC_PIC_KEY_MSTR")
        cur.execute("ALTER TABLE irb.PIC WITH CHECK CHECK CONSTRAINT FK__PIC__UPLD_KEY_MSTR")
        conn.commit()
