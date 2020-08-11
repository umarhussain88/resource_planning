#!/usr/bin/env python
# coding: utf-8


import sys, os, glob
from pathlib import Path

import pandas as pd
import numpy as np
from datetime import datetime
import shutil


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


# ## Leaver Holiday Deductions.
#
# ## As last years script (FY20) is quite robust it will be a blanket copy.
#
# * Simply a calculation of three files to calculate leaver holiday taken and any deductions to be applied to a shops holiday budget.
# * Starting with the Holiday Deduct File, get any leavers and thier latest contract.
# * Calculate the diff in the contract on the LHOD file and the Leaver file.
# * group the Holiday Taken file by Col, Shop and Week.
# * Final Step, if the balance owed is less than 0 then (holiday taken * balance owed) / [Holiday Taken by Date of Leaving], if greater than 0 then (balance owed * contract now)
# * Leaver file is used to get shop numbers and last contract.
#
#
# * Redone on 02/04/19 - Matches current process exactly. - Damian in agreement.
#


# Set Paths.

leaver_path = r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\LHOD\raw_data\leavers"

deduction_path = r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\LHOD\raw_data\deductions"

hol_taken_path = r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\LHOD\raw_data\hol_taken"

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

items_ = [leaver_path, deduction_path, hol_taken_path]


file_name, week_, day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


# read in latest files from these locations

leavers = pd.read_excel(newest(leaver_path), skiprows=1)

lhod = pd.read_excel(newest(deduction_path), skiprows=1)

hol_taken = pd.read_excel(newest(hol_taken_path), skiprows=1)

hol_taken_b = hol_taken


lhod.rename(columns={"Payroll Number": "Number"}, inplace=True)


## Get the last contract

last_cont = pd.merge(lhod, leavers[["Number", "Contract"]], on="Number", how="left")


## Lets Merge the Week Number on Termination Date ##

df = pd.merge(
    last_cont,
    dates[["date", "retail_ops_week"]],
    left_on=last_cont["Termination Date"].dt.normalize(),
    right_on="date",
    how="inner",
).drop(columns=["date"])


## Drop Stores with NA, this is from my process notes ###
"""
1. Create a list of Payroll Numbers from the LHOD File,
2. Group by Payroll Numbers of Holiday Taken and Sum the Holiday Hours
3. do a left merge on matching keys to get the sum of Holiday Hours and hol_taken Store Number
4. Merge the Sum of Holiday Hours onto the main Data Frame

"""

hol_taken.dropna(subset=["Location Ledger Code"], inplace=True)


hol_taken = hol_taken.groupby(["Number"])["Hours1"].sum().reset_index()


hol_taken_1 = pd.DataFrame({"Number": list(df["Number"])})


hol_taken_1 = pd.merge(hol_taken_1, hol_taken, on=["Number"], how="left")


# I have no idea why I wrote it like this - most likely following T's logic no time to refactor.

hol_taken_1 = hol_taken_1[["Number", "Hours1"]]

## as the Holiday Taken is a group by we need to remove the duplicate keys (Payroll Numbers)
# so we don't duplicate data in our main DF ##

hol_taken_1 = hol_taken_1.drop_duplicates(subset="Number").fillna(0)


## Merge this back onto our main DF ##

df = pd.merge(df, hol_taken_1, on="Number", how="left")

## Where there is no location for the LHOD Record, we use the actual Normal Weekly Contract from the LHOD report ##

df.loc[df.Contract.isnull(), ["Contract"]] = df["Weekly Contract"]

# All Contracts minus Contract at date of leaving


df["Contract Difference"] = df["Contract"] - df["Weekly Contract"]

df.loc[df["Contract Difference"].isna(), ["Contract Difference"]] = df["Contract"]


## Work out Deduction, this is done if the Colleagues Balance was
##lower than 0, then the Holiday Hours Taken multiplied by their Balance Divided by their Holiday Taken. ##
## If Balance > 0 then the Balance is multiplied by the most Recent Contract (taken from the Leaver file) ##


df["Deduction"] = np.where(
    df["Balance"] < 0,
    (df["Hours1"] * df["Balance"]) / df["Holiday Taken"],
    df["Balance"] * df["Contract"],
)


## use np.ceil to round this up to the nearest 0.25

df["Deduction"] = np.ceil(df["Deduction"] * 4) / 4


## Take the ABS of the Deduction ##

df["Final Deducts"] = np.where(df["Contract Difference"] == 0, abs(df["Deduction"]), 0)


## Take Leavers Shop Numbers ##

leavers = leavers.loc[leavers.Number.isin(df.Number)]


df = pd.merge(df, leavers[["Number", "Location"]], on="Number", how="left").rename(
    columns={"Location": "Location on Leave"}
)


## Get Holiday Taken Shop Number ##
## For YTD these aren't used at all ##

hol_taken_b = hol_taken_b.loc[hol_taken_b.Number.isin(df.Number)]
hol_taken_b = hol_taken_b.drop_duplicates(subset=["Number"], keep="last")

df = pd.merge(
    df, hol_taken_b[["Number", "Location Ledger Code"]], on="Number", how="left"
).rename(columns={"Location Ledger Code": "HT Last Location"})


## Workout PayCodes ##

df["PayCode"] = np.where(df["Deduction"] < 0, "LHOD", "LHOP")


lhod_final = df[
    ["retail_ops_week", "Termination Date", "Location on Leave", "Final Deducts"]
]


df["DFPaycode"] = df["PayCode"]


df1 = df[
    ["retail_ops_week", "Location on Leave", "PayCode", "DFPaycode", "Final Deducts"]
]


cols = ["retail_ops_week", "Store", "DFPaycode", "PayCode", "Hours"]


df1.columns = cols


# get today's date

datetime.today().strftime("%Y-%m-%d")

today = dates.loc[dates.date == datetime.today().strftime("%Y-%m-%d")]

week_ = int(today["week"])

day_ = int(today["posting_day"])

file_name = f"lhod_week_{week_}_day_{day_}.xlsx"


os.chdir(r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\LHOD\outputs")
writer = pd.ExcelWriter(file_name + "lhod.xlsx")
df.to_excel(writer, "raw_data", index=False)
df1.to_excel(writer, "lhod_deductions", index=False)
writer.save()
writer.close()

