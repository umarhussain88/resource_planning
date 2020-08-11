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


# ## Training Credits FY21
#
# * This script maniuplates course_credits and adds them into SQL.
#
# * We first merge all the files into one
#
# * we then drop duplicates based on certain criterion.
#
# * we allocate all the correct hours to each course.
#
# * we save a file for Jonty for FY21.
#
#


training_credits_raw = (
    r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Extra Hours\raw_data"
)


# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql(
    "SELECT Shop as store, Location, Area, Division from structure_tab", engine
)


file_name, week_, day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


#


# self defined function to grab multile file types.


def get_files(extensions, path_to_search):
    all_files = []
    for ext in extensions:
        all_files.extend(Path(path_to_search).glob(ext))
    return all_files


#


# lets pass all the files into a list

training_files = get_files(["*.csv", "*.xlsx"], training_credits_raw)


# lets split out the wsadmin report and remove it from the main list as the size of the data is different.

for file in training_files:
    if "wsadmin" in str(file):
        appren_file = file
        training_files.remove(file)

dfs = []
for file in training_files:
    n = str(file).split(".")[-1]
    if n == "csv":
        dfs.append(pd.read_csv(file))
    else:
        dfs.append(pd.read_excel(file))


# ## Step 1 - Training Courses (non apprentice)


training_courses = pd.concat(dfs)


# parse column names to lower case and replace spaces with _

training_courses.columns = [
    cols.lower().replace(" ", "_") for cols in training_courses.columns
]


# Remove cancelations - we don't credit or care about these.

training_courses = training_courses.loc[training_courses["status"] != "User Cancelled"]


## lets remove duplicates from non-aspire courses.

## The logic is to remove duplicates if they aren't booked or fully attended.
## People usually double book after they were accepted - we only want to credit those have have been booked - ie.
## approved by their store managers and remove those are 'pending' to be approved.

non_aspire = training_courses.loc[
    (training_courses["course_name"].str.contains("Aspire") == False)
    & (
        ~training_courses.duplicated(["username", "course_name"], keep=False)
        | training_courses["status"].ne("Booked", "Fully Attended")
    )
]


## Split out aspire so we can re-merge these into one file.

aspire = training_courses.loc[
    (training_courses["course_name"].str.contains("Aspire") == True)
]


training_cleaned = pd.concat([aspire, non_aspire])

training_cleaned["date"] = pd.to_datetime(
    training_cleaned["session_start_date"], dayfirst=True
)


cols_to_keep = [
    "username",
    "user's_fullname",
    "user's_organisation_name",
    "course_name",
    "date",
    "status",
]


# remove columns that we don't need.

training_cleaned = training_cleaned[cols_to_keep].copy()


#


## adding one year to ensure cal works for next year.

training_cleaned["date"] = training_cleaned["date"] + pd.Timedelta(days=365)


# # Handle Missing Courses from the raw_reports

#


os.chdir(r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Extra Hours\course_mapper")

current_course_names = training_cleaned[["course_name"]].drop_duplicates()

c_mapper = pd.read_excel(
    r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Extra Hours\course_mapper\course_mapper_master.xlsx"
)

checker = c_mapper["course_name"].tolist()

print("course_mapper_updated")


#


missing_courses = current_course_names.loc[
    ~current_course_names["course_name"].isin(checker)
]["course_name"].tolist()


#


if len(missing_courses) > 0:
    pd.DataFrame({"Missing Courses": missing_courses}).to_excel(
        "Missing_Courses.xlsx", index=False
    )
    for name in missing_courses:
        print(f"{name} is missing from the course_mapper_master_file please add it")
    print(
        """The missing course names have been saved to the course_mapper_folder - please use the exact name with -\nno leading or trailing spaces in the master_file"""
    )
else:
    print("All reports are present in the report")


# # Finish cleaning the raw_reports and add in the correct hours.

#


training_cleaned = pd.merge(training_cleaned, dates, on="date", how="inner")


#


training_cleaned["store"] = (
    training_cleaned["user's_organisation_name"]
    .str.extract("(\d+)")
    .fillna(0)
    .astype(int)
)


#


training_cleaned.drop("user's_organisation_name", axis=1, inplace=True)


#


training_cleaned = pd.merge(training_cleaned, c_mapper, on="course_name", how="left")


#


training_cleaned.loc[
    training_cleaned["status"].isin(["No show", "Declined", "Requested"]), "hours"
] = 0


#


## Create the reason column - this is for the SM's information and clarity.

training_cleaned["reason"] = (
    training_cleaned["course_name"]
    + ": "
    + training_cleaned["user's_fullname"]
    + " \\\\ "
    + training_cleaned["status"]
    + ":"
    + training_cleaned["date"].dt.strftime("%a %b %y")
)


#


training_cleaned.rename(columns={"course_name": "type"}, inplace=True)


#


training_final = training_cleaned[
    ["username", "type", "store", "retail_ops_week", "reason", "hours"]
]


# # Apprenticeships.

#


# Same logic as above, but calcs are done by start + end time, need to use some handy regex (regular expressions) for this.

appr = pd.read_excel(appren_file)


#


appr.columns = [cols.lower().replace(" ", "_") for cols in appr.columns]

appr["store"] = (
    appr["user's_organisation_name"].str.extract("(\d+)").fillna(0).astype(int)
)


#


appr["date"] = pd.to_datetime(appr["session_start_date"], dayfirst=True)

appr["date"] = appr["date"] + pd.Timedelta(days=365)


#


appr = appr.loc[appr["status"] != "User Cancelled"].copy()


#


# extract start + end times using regex to calculate the differences as python datetime objects.

a = pd.to_timedelta(
    pd.to_datetime(
        appr["session_start_time"].str.extract(
            r"\b((1[0-2]|0?[1-9]):([0-5][0-9]) ([AaPp][Mm]))", expand=False
        )[0]
    ).dt.strftime("%H:%M:%S")
)

b = pd.to_timedelta(
    pd.to_datetime(
        appr["session_finish_time"].str.extract(
            r"\b((1[0-2]|0?[1-9]):([0-5][0-9]) ([AaPp][Mm]))", expand=False
        )[0]
    ).dt.strftime("%H:%M:%S")
)


#


# round up to 15min intervals and calc hours.
# b = end time, a = start time

hours = (b - a).dt.round(freq="15min").dt.seconds / 3600


#


appr["hours"] = hours


#


appr.loc[appr["status"].isin(["Requested", "No show"]), "hours"] = 0


#


appr["reason"] = (
    "Apprenticeship "
    + appr["user's_fullname"]
    + ": "
    + appr["status"]
    + " \\\\"
    + appr["date"].dt.strftime("%a %b %y")
    + "- "
    + pd.to_datetime(pd.to_timedelta(a)).dt.strftime("%X")
    + " : "
    + pd.to_datetime(pd.to_timedelta(b)).dt.strftime("%X")
)


#


appr["type"] = "apprenticeships"


#


appr = pd.merge(appr, dates, on="date", how="inner")


#


final_appren = appr[["username", "type", "store", "retail_ops_week", "reason", "hours"]]

print("The current breakdown of hours by status:")
print(appr.groupby("status")["hours"].sum())


# # Concat both files and add in columns for database.
#
# * final bit of prep before saving file down.

#


df = pd.concat([final_appren, training_final])


#


df["store"] = df["store"].fillna(0).astype(int).astype(str).str.zfill(4)


#


df["shop"] = df["store"].astype(int)


#


df["CostCentre"] = 5001


#


df["Week Number"] = df["retail_ops_week"] - 2100


#


df["BusinessFunction"] = "Hub Team"
df["Owner"] = "Hub Team"
df["Rate"] = 9
df.rename(columns={"username": "EmployeeNumber"}, inplace=True)

df = df[
    [
        "store",
        "shop",
        "retail_ops_week",
        "hours",
        "reason",
        "CostCentre",
        "type",
        "Rate",
        "Owner",
        "BusinessFunction",
        "EmployeeNumber",
        "Week Number",
    ]
].copy()


#


df.columns = pd.read_sql("SELECT TOP 1 * From extrahoursdetails", engine).columns


#


os.chdir(r"S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Extra Hours\outputs")


#


df.to_excel("extra_hours_" + file_name)


#


training_files = get_files(["*.csv", "*.xlsx"], training_credits_raw)

for file in training_files:
    file.rename(Path(file.parent, f"{file_name}_{file.stem}{file.suffix}"))


#


training_files = get_files(["*.csv", "*.xlsx"], training_credits_raw)

for file in training_files:
    shutil.move(
        str(file),
        os.path.join(str(file.parent) + "\\processed", str(file).split("\\")[-1]),
    )

