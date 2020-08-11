#!/usr/bin/env python
# coding: utf-8


import sys, os
from pathlib import Path
from time import sleep

sys.path.append(
    os.path.abspath(
        r"S:\Data\Stores Payroll\FY21\99_Master Scripts (DO NOT EDIT)\dB_Connector"
    )
)

from connector import *

sys.path.append(
    os.path.abspath(
        r"S:\Data\Stores Payroll\FY21\99_Master Scripts (DO NOT EDIT)\common_functions"
    )
)

from halfords_functions import newest, add_years, halfords_week


import pandas as pd
from datetime import datetime
import shutil


# # Schedule Efficiency #
#
# ## Finalised and Agreed ##
#
# 02/04/19
#
# * Format finalised script does as follows
#
# * Reads in latest file from rawdata in Schedule Efficiency
#
# * as all scores are pre-calcualted from DF we essentially create an empty frame for every shop for every week of the year.
#
# * we merge in the schedule efficiency report and any missing values (Where shops have not published rotas) are coded as 0
#
# * we calculate the unplanned week from the current week + 4.
#
# * we save files down for Retail Finance on Friday's & Monday's.
#
# --
#
# Damian & Umar have agreed this.
#

# # Step 1 - Read in Dates & Store's.
#
#


# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql(
    "SELECT Shop as store, Location, Area, Division from structure_tab", engine
)


file_name, week_, day_ = halfords_week(dates)
unplanned_week = week_ + 4
print(f"We are {week_} weeks away from FY21")


print(f"The current unplanned week is {unplanned_week}")


# se = schedule_efficiency

se_path_daily = r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Schedule Efficiency\raw_data\daily_files"


try:
    se_daily = pd.read_excel(newest(se_path_daily), skiprows=1, parse_dates=["Week"])
except PermissionError:
    print(
        "There are no files in this folder, skiping this step. If there is a file, hit refresh in your windows console,\n or make a copy of the file, at times there are caching errors on windows machines."
    )


columns_to_use = ["Location", "Week", "Zone", "Schedule Efficiency"]


se_daily = se_daily[columns_to_use].copy()


se_daily["store"] = se_daily["Location"].str.extract("(\d+)").astype(int)


se_daily.rename(columns={"Week": "date"}, inplace=True)


se_daily = pd.merge(se_daily, dates, on="date", how="left").copy()


se_daily["Schedule Efficiency"] = (
    se_daily["Schedule Efficiency"].str[:5].astype(float).divide(100).fillna(0)
).copy()


structure["retail_ops_week"] = 2101

structure = pd.concat([structure] * 52)

structure["retail_ops_week"] = structure["retail_ops_week"].add(
    structure.groupby(["store"]).cumcount()
)


se_daily = pd.merge(
    structure, se_daily[["store", "Schedule Efficiency", "retail_ops_week"]], how="left"
).fillna(0)


se_daily_ct = pd.crosstab(
    se_daily["store"],
    se_daily["retail_ops_week"],
    se_daily["Schedule Efficiency"],
    aggfunc="sum",
).reset_index()

se_daily_ct["store"] = se_daily_ct["store"].astype(int).astype(str).str.zfill(4)

dtypes = {"Store": sa.types.VARCHAR(length=50)}

se_daily_ct.rename(columns={"store": "Store"}, inplace=True)


se_daily_ct.to_sql(
    "schedule_efficiency", con=engine, if_exists="replace", index=False, dtype=dtypes
)

print("Schedule Efficiency Updated on SQL")


unplanned_hours = se_daily.loc[
    (se_daily["retail_ops_week"] == unplanned_week + 2100)
    & (se_daily["Schedule Efficiency"] == 0)
]


os.chdir(
    r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Schedule Efficiency\daily_reports"
)


writer = pd.ExcelWriter(f"{file_name}schedule_efficiency.xlsx")
se_daily_ct.to_excel(writer, "se_crosstab", index=False)
unplanned_hours.to_excel(writer, "unplanned_stores", index=False)
writer.save()


# # Weekly Pitstop Files.


weekly_path = r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Schedule Efficiency\raw_data\weekly_files"


se_weekly = pd.read_excel(newest(weekly_path), skiprows=1)


se_weekly["Efficiency Score"] = (
    se_weekly["Efficiency Score"].str[:5].astype(float).divide(100).fillna(0)
)


se_weekly["Location"] = se_weekly["Location"].str.extract("(\d+)").astype(int)


se_weekly.drop(["Unnamed: 4", "Zone"], axis=1, inplace=True)


se_weekly["DayId"] = pd.to_datetime(se_weekly["DayId"], dayfirst=True)


se_weekly.columns = ["store", "date", "score"]


se_weekly = pd.merge(se_weekly, dates, on="date", how="inner").copy()


structure["posting_day"] = 1


structure = pd.concat([structure] * 7)


structure["posting_day"] = structure["posting_day"].add(
    structure.groupby(["store", "retail_ops_week"]).cumcount()
)


se_weekly_final = (
    pd.merge(
        structure, se_weekly, on=["store", "retail_ops_week", "posting_day"], how="left"
    )
    .fillna(0)
    .copy()
)


pitstop = se_weekly_final.loc[(se_weekly_final["retail_ops_week"] == week_ + 2100)]


pit_stop = pitstop[["store", "retail_ops_week", "day", "score"]]

se_daily_pitstop = se_daily[["store", "retail_ops_week", "Schedule Efficiency"]].copy()
se_daily_pitstop = se_daily_pitstop.loc[
    se_daily_pitstop["retail_ops_week"] == week_ + 2100
]


os.chdir(
    r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Schedule Efficiency\pitstop_reports"
)

writer = pd.ExcelWriter(file_name + "pitstop.xlsx")
pit_stop.to_excel(writer, "daily_scores", index=False)
se_daily_pitstop.to_excel(writer, "week_score", index=False)
writer.save()


paths = [weekly_path, se_path_daily]


for file in paths:
    for file in Path(file).glob("*.xlsx"):
        file.rename(Path(file.parent, f"{file_name}_{file.stem}{file.suffix}"))


for files in paths:
    for file in Path(files).glob("*.xlsx"):
        print("Moving", str(file).split("\\")[-1])
        shutil.move(
            str(file), os.path.join(files + "\\processed", str(file).split("\\")[-1])
        )

