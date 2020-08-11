#!/usr/bin/env python
# coding: utf-8

# # Performance Hours Step 2
#
# * Create Tabular form
#
# * Write to SQL.
#
# * Create a traceable weekly log.


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
import re
import numpy as np


# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql("SELECT Shop as store from structure_tab", engine)


file_name, week_, day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


# # Read in latest Perf.


perf_loc = r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Actual Sales - Performance Hours\outputs"


perf_ = pd.read_excel(newest(perf_loc), sheet_name=0)


perf_.rename(columns={"Store": "store"}, inplace=True)


# get perf from SQL.

perf = pd.read_sql("SELECT * from perf_hours_tabular", engine)

os.chdir(
    r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Actual Sales - Performance Hours\sql_extract"
)

perf.to_csv(file_name + "perf_extract.csv", index=False)

# Merge with latest file.
perf_m = pd.merge(
    perf, perf_[["store", "Week", "Actual Perf"]], on=["store", "Week"], how="left"
)

# Replace current week with 0 and write in actual scores.
perf_m.loc[perf_m["Week"] == perf_["Week"][0], "Perf"] = perf_m["Actual Perf"]

# Drop column.
perf_m.drop("Actual Perf", axis=1, inplace=True)


d_types = {"Store": sa.types.VARCHAR(length=50)}

print(f"This weeks Perf is {perf_['Actual Perf'].sum()}")

while True:
    print(f"Your Week Listed in the File is {perf_['Week'][0]}")
    print("Do you wish to write these into SQL?")
    cmd = input("Please enter [Y]es or [N]o to continue")
    cmd.lower()
    if cmd == "y":
        perf_m.to_sql(
            "perf_hours_tabular", engine, schema="dbo", if_exists="replace", index=False
        )
        perf_m["Store"] = perf_m["store"].astype(int).astype(str).str.zfill(4)
        perf_ct = pd.crosstab(
            perf_m["Store"], perf_m["Week"], perf_m["Perf"], aggfunc="sum"
        ).reset_index()
        perf_ct.to_sql(
            "perf_hours",
            engine,
            schema="dbo",
            index=False,
            if_exists="replace",
            dtype=d_types,
        )
        print("Finished Writing to SQL.")
        print("Crosstab Finished.")
        break
    elif cmd == "n":
        print("Exiting Program")
        break
    else:
        print("Please Enter [Y] or [N]")


for file in Path(perf_loc).glob("*.xlsx"):
    shutil.move(
        str(file),
        os.path.join(str(file.parent) + "\\processed", str(file).split("\\")[-1]),
    )

