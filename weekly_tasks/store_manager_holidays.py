#!/usr/bin/env python
# coding: utf-8

# # Store Manager Holidays.
#
#
# * Create the output file for SM holidays and write to SQL.
#
# * Have some issues with this to old SQL server drivers & the depcricated TEXT function in SQL server.


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

structure = pd.read_sql("SELECT Shop as store, Area from structure_tab", engine)


file_name, week_, day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


hol_data = (
    r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Store Manager Holidays\raw_data"
)


df = pd.read_excel(newest(hol_data), skiprows=1)


df["Date"].min()


# Select relevant columns.

cols = ["Site Name", "Display Name", "Date", "Number"]

## Slice relevant coluns ##

df = df[cols].copy()


# This is a badly setup report so some data wrangling is required.


df["Site Name"].ffill(inplace=True)

df["store"] = df["Site Name"].str.extract(("(\d+)")).astype(int)


df.dropna(subset=["Display Name"], inplace=True)


df = pd.merge(df, structure[["store", "Area"]], on="store", how="left")


df.rename(columns={"Date": "date"}, inplace=True)


df = pd.merge(df, dates, on="date", how="inner")


df["start"] = df["date"].dt.strftime("%d/%m/%Y")


df.rename(
    columns={"Display Name": "title", "store": "Stores", "date": "Date"}, inplace=True
)

df = df.drop_duplicates(subset=["title", "start"], keep="last")


df = df[["start", "Area", "title", "Date", "Stores", "Number"]]


dtypez = {
    "start": sa.types.VARCHAR,
    "Area": sa.types.BIGINT,
    "title": sa.types.VARCHAR(length=225),
    "Date": sa.types.NVARCHAR(length=50),
    "Stores": sa.types.VARCHAR(length=50),
    "Number": sa.types.BIGINT,
}

df["Stores"] = df["Stores"].astype(str).str.zfill(4)

df["Number"] = df["Number"].astype(int)

df["Area"] = df["Area"].astype(int)


df.to_sql(
    "events", con=engine, if_exists="replace", index=False, schema="dbo", dtype=dtypez
)


print("Holidays Updated, annie is NOT cool.")


for file in Path(hol_data).glob("*.xlsx"):
    file.rename(Path(file.parent, f"{file_name}_{file.stem}{file.suffix}"))


for file in Path(hol_data).glob("*.xlsx"):
    shutil.move(
        str(file),
        os.path.join(str(file.parent) + "\\processed", str(file).split("\\")[-1]),
    )


print("Files saved and moved into processed folders.")

