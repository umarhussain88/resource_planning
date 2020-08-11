#!/usr/bin/env python
# coding: utf-8

# # Simple script to Create a Crosstab & the KPI File for Dayforce.


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

from halfords_functions import newest, halfords_week

import pandas as pd
from datetime import datetime
import shutil
import numpy as np


# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql("SELECT Shop as store from structure_tab", engine)


file_name, week_, day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


# Create an empty dataframe with a value for every week of the year.

structure["Type"] = "Extra"
structure["WeekYear"] = 2020001
structure = pd.concat([structure] * 52)

structure["WeekYear"] = structure["WeekYear"].add(
    structure.groupby(["store"]).cumcount()
)


# Read in the Structure Tab as an aggregate query.

xh20 = pd.read_sql(
    "SELECT shop as store, sum(hours) as Hours, [Week Number] from extraHoursDetails group by [Week Number], Shop",
    engine,
)

xh20["Week Number"] = xh20["Week Number"].astype(str).str.zfill(3)


xh20["WeekYear"] = "2020" + xh20["Week Number"]


xh20["WeekYear"] = xh20["WeekYear"].astype(int)


structure["store"] = structure["store"].astype(int)


finalDF = (
    pd.merge(
        structure,
        xh20[["store", "WeekYear", "Hours"]],
        on=["store", "WeekYear"],
        how="left",
    )
    .fillna(0)
    .sort_values(["store", "WeekYear"])
)

finalDF = finalDF[["Type", "WeekYear", "store", "Hours"]].reset_index(drop=True)


os.chdir(r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Extra Hours\dayforce_kpi_files")

finalDF.to_csv(f"{file_name}_dayforce_kpi_unprocessed.csv", index=False, header=None)

os.chdir(r"G:\MFRAME\FTPDATA\Dayforce\PRD\KPIHours")


print(f"Do you wish to save the KPI file to {os.getcwd()}")

cmd = input("[Y] or [N]")

while True:
    if cmd.lower().strip() == "y":
        print(f"saving file to {os.getcwd()}")
        the_path = '"G:\MFRAME\FTPDATA\Dayforce\PRD\KPIHours'
        print(
            f"file saved to : {the_path} please email the IT helpdesk asking them to process the file"
        )
        break
    elif cmd.lower().strip() == "n":
        print("Saving file to \\extra hours\dayforce_kpi_files only")
        break
    else:
        print("Please enter Y or N")


xh20 = "SELECT * from extraHoursDetails"

xh20 = pd.read_sql(xh20, engine)

st = pd.read_sql("SELECT Shop as Store from structure_tab", engine)

## Create a Week Column and set to Week 1 ##
st["Week"] = 2101


st = pd.concat([st] * 52)

st["Store"] = st["Store"].astype(int).astype(str).str.zfill(4)

st["Week"] = st.Week.add(st.groupby(["Store"]).cumcount())

xh20ct = pd.merge(
    st, xh20[["Store", "Week", "Hours"]], on=["Store", "Week"], how="left"
).fillna(0)

xh20ct = pd.crosstab(
    xh20ct["Store"], xh20ct["Week"], xh20ct["Hours"], aggfunc="sum"
).reset_index()

xh20ct.columns = xh20ct.columns.astype(str)

xh20ct.to_sql(
    "extraHours",
    con=engine,
    schema="dbo",
    if_exists="replace",
    index=False,
    dtype={"Store": sa.types.VARCHAR(length=255)},
)

print("Crosstab Updated.")

