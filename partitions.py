import dagster as dg
from datetime import datetime
from dagster_tutorial.defs.assets import constants

start_date = datetime.strptime(constants.START_DATE, constants.DATE_FORMAT)
start_date_str = start_date.strftime(constants.DATE_FORMAT)

hourly_partitions = dg.HourlyPartitionsDefinition(
    start_date=start_date,
)

daily_partition = dg.DailyPartitionsDefinition(
    start_date=start_date,
)

weekly_partition = dg.WeeklyPartitionsDefinition(
    start_date=start_date,
    day_offset=0,  # Start weeks on Sunday
)

monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=start_date,
)

