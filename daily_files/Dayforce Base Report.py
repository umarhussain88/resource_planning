#!/usr/bin/env python
# coding: utf-8

# # Master Script for FY21 Hours Used & Holiday Spend.
#
#
# Steps as follows
#
# * Simple script that wrangles data into 3 outputs:
# * Hours Used - Holiday Spent - PayCodeDetail (this is the detail behind the hours used table)
#
# * Code will be documented for readability - as data is very small only takes 3-5 seconds to create all three tables &
# * write to SQL.
#
# * We take a extract of every table for risk and to measure daily changes.
#
#
#
#


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


# # Read in relevant tables :
#
# * fy21 Calendar
# * Structure Tab
# * pay_code lookups.


# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql("SELECT Shop as store from structure_tab", engine)

# paycodes = payCodeLookups

paycodes = pd.read_sql("SELECT * from paycodes", engine)


file_name, week_, day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


# ### Read in Newest Data


base_report_path = (
    r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Dayforce Base Report\Raw Data"
)


## Read in Report and set column names, then drop the last column, not sure why this keeps reading in..

base_report = pd.read_excel(
    newest(base_report_path),
    header=None,
    skiprows=2,
    names=["date", "store", "pay_code", "hours", ""],
)

base_report = base_report.iloc[:, :-1]

# set date time column.

base_report["date"] = pd.to_datetime(base_report["date"], dayfirst=True)


# inner join to only return matches, we don't care about data outside of the Financial Year.

paycode_hours = pd.merge(base_report, dates, on="date", how="inner")


## add in paycodes and whether we use them for calculations - Ideally this should be redone with Finance input so
# we are aligned with the business spend.

paycode_hours = pd.merge(
    paycode_hours,
    paycodes[
        [
            "dfpaycode",
            "title",
            "timesheet",
            "add",
            "deduct",
            "exclude",
            "holadd",
            "holded",
        ]
    ],
    left_on="pay_code",
    right_on="dfpaycode",
    how="left",
)


# change negative values to positive,
## some store managers enter in negative values for timesheet entries.. no validation on DF.

paycode_hours["hours"] = abs(paycode_hours["hours"])

## we now split out the single dataframe into two, I could make this more elegant by changing the
## paycode table, don't have time for this.

# ## Step 1 - Create Deduct & Hoursused Tables.

# we use the timesheet column & deduct column to break out the hours spend & deductions.

hours_spend = (
    paycode_hours.loc[paycode_hours["timesheet"] == "Y"]
    .groupby(["store", "retail_ops_week"])["hours"]
    .sum()
    .reset_index()
)


deduct_spend = (
    paycode_hours.loc[paycode_hours["deduct"] == "Y"]
    .groupby(["store", "retail_ops_week"])["hours"]
    .sum()
    .reset_index()
)


## lets merge these now and rename the columns.

hours_spend_summary = (
    pd.merge(hours_spend, deduct_spend, on=["store", "retail_ops_week"], how="left")
    .rename(columns={"hours_y": "deduct_hours", "hours_x": "hours"})
    .fillna(0)
)


# The Total Hours Charged

## the deductions are agreed business rules between ops & the people team in regards to what we backfill & what we don't.
## the holiday spend is accounted for in the holiday budget so not charged to their available hours. (or worked hours.)

hours_spend_summary["total_hours_charged"] = (
    hours_spend_summary["hours"] - hours_spend_summary["deduct_hours"]
)

# ## Step 2 -  Leaver Holiday Deductions LHOD.

## This is a lengthy script - and is only done weekly, but we need to account for it daily.

## We read in the latest file from the LHOD area - and calculate it here.

lhod_path = r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\LHOD\outputs"

lhod = pd.read_excel(newest(lhod_path), sheet_name=-1)

lhod.columns = ["retail_ops_week", "store", "dfpaycode", "pay_code", "hours"]

## Lets concat this to our main_paycode dataframe to work out the holiday & deductions.

paycode_hours = pd.concat(
    [
        paycode_hours[["retail_ops_week", "store", "dfpaycode", "pay_code", "hours"]],
        lhod,
    ]
).copy()


# # Step 3 - Holiday Time #
#
# ## Calculate Holiday Hours with PayCodes ##
#
# * Changes from FY19
# * This will include HOLC which wasn't included in FY19


# Merge the paycode deduct data again.

paycode_hours = (
    pd.merge(
        paycode_hours,
        paycodes[
            [
                "dfpaycode",
                "title",
                "timesheet",
                "add",
                "deduct",
                "exclude",
                "holadd",
                "holded",
            ]
        ],
        left_on="pay_code",
        right_on="dfpaycode",
        how="left",
    )
    .drop("dfpaycode_y", axis=1)
    .rename(columns={"dfpaycode_x": "dfpaycode"})
    .copy()
)

holiday = (
    paycode_hours.loc[paycode_hours["holadd"] == "Y"]
    .groupby(["retail_ops_week", "store"])["hours"]
    .sum()
    .reset_index()
    .rename(columns={"hours": "holiday_hours"})
)


leaver_hol = (
    paycode_hours.loc[paycode_hours["holded"] == "Y"]
    .groupby(["retail_ops_week", "store"])["hours"]
    .sum()
    .reset_index()
    .rename(columns={"hours": "lhod_hours"})
)


# # Final Step - Create the SQL Tables and File I/O Operations.
#
# * Create SQL Tables
# * Save file down to relevant areas.
# * Move file from raw data to processed.
## All - Hours.

final_hours = pd.merge(hours_spend_summary, structure, on="store", how="right")

final_hours = (
    pd.merge(final_hours, holiday, on=["store", "retail_ops_week"], how="left")
    .fillna(0)
    .copy()
)

final_hours = (
    pd.merge(final_hours, leaver_hol, on=["store", "retail_ops_week"], how="left")
    .fillna(0)
    .copy()
)

## Workout Total Holiday Hours for Holiday CrossTab.

final_hours["total_holiday_hours"] = (
    final_hours["holiday_hours"] - final_hours["lhod_hours"]
)

final_hours["retail_ops_week"] = final_hours["retail_ops_week"].astype(int)


hours_used_ct = pd.concat([structure] * 52)

hours_used_ct["retail_ops_week"] = 2101

hours_used_ct["retail_ops_week"] = hours_used_ct["retail_ops_week"].add(
    hours_used_ct.groupby(["store"]).cumcount()
)

# Create a blank dataframe with everyshop for every week of the year


hours_used_ct = pd.merge(
    hours_used_ct,
    final_hours[["store", "retail_ops_week", "total_hours_charged"]],
    on=["store", "retail_ops_week"],
    how="left",
).fillna(0)


## Hours Used Crosstabs.
hours_used_ct = (
    pd.crosstab(
        hours_used_ct["store"],
        hours_used_ct["retail_ops_week"],
        hours_used_ct["total_hours_charged"],
        aggfunc="sum",
    )
    .reset_index()
    .fillna(0)
)


hours_used_ct["store"] = hours_used_ct["store"].astype(int).astype(str).str.zfill(4)

hours_used_ct.rename(columns={"store": "Store"}, inplace=True)

# save file down :

os.chdir(
    r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Dayforce Base Report\HoursUsed FY20"
)


hours_used_ct.to_excel(f"{file_name}_hours_used.xlsx", index=False)


# ### Paycode Table.
paycode_list = pd.DataFrame(paycodes.dropna(subset=["dfpaycode"])["dfpaycode"].tolist())

# generate a list of dayforce paycodes.
structure["retail_ops_week"] = 2101

# set week to one.
## awesome line of code, does a caretesian join to create a paycode entry for every store for week 1.

paycode_detail = structure.assign(a=1).merge(paycode_list.assign(a=1)).drop("a", 1)

## 22 * 450 is 9900 is the code did as expected. - we do this as asp-classic/sql needs to an entry for every potential value
## even if that value is 0.

paycode_detail = pd.concat(
    [paycode_detail] * 52
)  # create an entry for every week in the year.

paycode_detail.rename(columns={0: "dfpaycode"}, inplace=True)

paycode_detail["retail_ops_week"] = paycode_detail["retail_ops_week"].add(
    paycode_detail.groupby(["store", "dfpaycode"]).cumcount()
)

paycode_d = (
    paycode_hours.groupby(["store", "dfpaycode", "retail_ops_week"])["hours"]
    .sum()
    .reset_index()
)


paycode_detail = (
    pd.merge(
        paycode_detail,
        paycode_d,
        on=["store", "dfpaycode", "retail_ops_week"],
        how="left",
    )
    .fillna(0)
    .copy()
)

## PayCode Detail ##
## this is just a groupby on the Analysis Table ##


## Create CrossTab ##
paycode_details = (
    pd.crosstab(
        [paycode_detail["store"], paycode_detail["retail_ops_week"]],
        paycode_detail["dfpaycode"],
        paycode_detail["hours"],
        aggfunc=sum,
    )
    .fillna(0)
    .reset_index()
)


paycode_details = (
    pd.merge(paycode_details, final_hours, on=["store", "retail_ops_week"], how="left")
    .fillna(0)
    .copy()
)

## set metadata type for SQL Server ##


paycode_details["holiday_hours"] = paycode_details["HOL"]
paycode_details["Leaver Holiday Pay"] = paycode_details["LHOP"]

paycode_details.rename(
    columns={
        "hours": "Actual Dayforce Spend",
        "deduct_hours": "deductHours",
        "total_hours_charged": "Total Hours charged",
        "holiday_hours": "Holiday Hours",
        "lhod_hours": "Leaver Holiday Deducts",
        "total_holiday_hours": "Total Holiday Hours",
        "store": "Store",
        "retail_ops_week": "YearWeek",
    },
    inplace=True,
)


cols = [
    "Store",
    "YearWeek",
    "FERTILITY TREATMENT",
    "BRK",
    "AUTHORISED UNPAID ABSENCE",
    "BEREAVEMENT",
    "EMERGENCY FAMILY LEAVE",
    "ADOPTION",
    "HOL",
    "HOLC",
    "JURY SERVICE",
    "MATERNITY",
    "MEDICAL APPOINTMENT",
    "OFFSITE OR TRAINING",
    "PATERNITY",
    "SHARED PARENTAL LEAVE",
    "SICKNESS",
    "SUSPENSION",
    "TIME OUT",
    "UNAUTHORISED UNPAID ABSENCE",
    "TA RESERVIST",
    "WRK",
    "LHOD",
    "LHOP",
    "Actual Dayforce Spend",
    "deductHours",
    "Total Hours charged",
    "Holiday Hours",
    "Leaver Holiday Deducts",
    "Leaver Holiday Pay",
    "Total Holiday Hours",
]


paycode_details = paycode_details[cols]


paycode_details["Store"] = paycode_details["Store"].astype(int).astype(str).str.zfill(4)


os.chdir(
    r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Dayforce Base Report\PayCodeDetail"
)


paycode_details.to_excel(f"{file_name}_paycode_detail.xlsx", index=False)


# # Holiday Crosstab


holiday_ct = pd.concat([structure] * 52)


holiday_ct["retail_ops_week"] = holiday_ct["retail_ops_week"].add(
    holiday_ct.groupby(["store"]).cumcount()
)

# Create a blank dataframe with everyshop for every week of the year.


holiday_ct = (
    pd.merge(
        holiday_ct,
        final_hours[["store", "retail_ops_week", "total_holiday_hours"]],
        on=["store", "retail_ops_week"],
        how="left",
    ).fillna(0)
).copy()


holiday_ct_final = pd.crosstab(
    holiday_ct["store"],
    holiday_ct["retail_ops_week"],
    holiday_ct["total_holiday_hours"],
    aggfunc="sum",
).reset_index()


holiday_ct_final["store"] = (
    holiday_ct_final["store"].astype(int).astype(str).str.zfill(4)
)


holiday_ct_final.rename(columns={"store": "Store"}, inplace=True)


# save file down


# save file down :

os.chdir(
    r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Dayforce Base Report\Holiday Taken"
)

holiday_ct_final.to_excel(f"{file_name}_holiday_taken.xlsx", index=False)


# # Write to SQL and move raw data.


## Write this to SQL

## Set meta_type TEXT is depcreiated in SQL server and gives errors.

dtypes = {"Store": sa.types.VARCHAR(length=50)}

hours_used_ct.to_sql(
    "hours_used",
    con=engine,
    schema="dbo",
    index=False,
    dtype=dtypes,
    if_exists="replace",
)

print(f"Hi {os.getlogin()}, the hours_used table has been updated")


holiday_ct_final.to_sql(
    "holiday_spend",
    con=engine,
    schema="dbo",
    index=False,
    dtype=dtypes,
    if_exists="replace",
)

print("holiday_tab detail updated")


paycode_details.to_sql(
    "paycode_detail",
    con=engine,
    schema="dbo",
    index=False,
    dtype=dtypes,
    if_exists="replace",
)

print("pay_code detail updated")


processed_path = r"S:\Data\Stores Payroll\FY21\01_Daily Tasks\Dayforce Base Report\Raw Data\processed"


for file in Path(base_report_path).glob("*.xlsx"):
    file.rename(Path(file.parent, f"{file_name}_{file.stem}{file.suffix}"))


for file in Path(base_report_path).glob("*.xlsx"):
    shutil.move(
        str(file),
        os.path.join(str(file.parent) + "\\processed", str(file).split("\\")[-1]),
    )

