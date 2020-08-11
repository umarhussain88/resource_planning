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

from halfords_functions import newest, halfords_week


import pandas as pd
from datetime import datetime
import shutil
import numpy as np


# # Write all Training Credits to Database.
#
# * Same process as FY20, as we don't have compliance from the training team, we will do the following :
#
# * Delete two weeks of Training Data from our SQL database.
#
# * add these in.
#
# * Update Crosstab.


# read database to create an extract.

extra_hours = pd.read_sql("SELECT * from extraHoursDetails", engine)


# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)


file_name, week_, day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


os.chdir(r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Extra Hours\sql_extracts")


extra_hours.to_excel(f"{file_name}_extra_hours_extract.xlsx", index=False)


training_credits = r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Extra Hours\outputs"


df = pd.read_excel(newest(training_credits))


df["Store"] = df["Store"].astype(str).str.zfill(4)


# # We get the current week and get the data from the previous two weeks.


print(f"We will be only writing in data greater than Week {week_-2}")


df = df.loc[df.Week >= week_ - 2].copy()


df["EmployeeNumber"] = pd.to_numeric(df["EmployeeNumber"], errors="coerce").fillna(0)


# # Set Metatypes for SQL Server.


xhtypes = {
    "Store": sa.types.VARCHAR(length=50),
    "Shop": sa.types.BIGINT,
    "Week": sa.types.BIGINT,
    "Hours": sa.types.FLOAT,
    "Reason": sa.types.VARCHAR(length=255),
    "CostCentre": sa.types.VARCHAR(length=255),
    "Type": sa.types.VARCHAR(length=50),
    "Rate": sa.types.FLOAT,
    "Owner": sa.types.VARCHAR(length=255),
    "BusinessFunction": sa.types.VARCHAR(length=255),
    "EmployeeNumber": sa.types.BIGINT,
    "Week Number": sa.types.BIGINT,
}


new = newest(os.getcwd())

### this is the SQL Query to delete all records for the Hub & Training Team in the future, this allows us to track changes
## and credit as needed, as compliance is an issue, the records are delete for the current week and two prior.
# Outliers will be dealt in a seperate script.

d = """DELETE extraHoursDetails

where BusinessFunction = 'Hub Team'

and Week >=""" + str(
    int(week_) - 2
)


t_path = Path(newest(training_credits))


# # Final Step - Write This to SQL.


## This code is quite shit, but works so I won't re-write it. Would recommend it be re-written so :
## it's clear.

while True:

    cmd = input(
        f"Is this the file used for Training Credits Correct ? {t_path.name} all HUB Credits after Week {week_-2} will be deleted [Y] or [N]"
    )
    if cmd == "y":
        cnxn.execute(d)
        cnxn.commit()
        df.to_sql(
            "extraHoursDetails",
            con=engine,
            schema="dbo",
            index=False,
            if_exists="append",
            dtype=xhtypes,
        )
        xh20 = "SELECT * from extraHoursDetails"
        xh20 = pd.read_sql(xh20, engine)
        sql = "SELECT Distinct(Store) from structure_tab"
        st = pd.read_sql(sql, engine)
        ## Create a Week Column and set to Week 1 ##
        st["Week"] = 2101
        st = pd.concat([st] * 52)
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
        print("Added to SQL")
        print("CrossTab Updated")
        break
    else:
        print("Exiting Program, run this again to ADD to SQL.")
        break


for file in Path(training_credits).glob("*.xlsx"):
    shutil.move(
        str(file),
        os.path.join(str(file.parent) + "\\processed", str(file).split("\\")[-1]),
    )

print("Training outputs moved into processed.")

