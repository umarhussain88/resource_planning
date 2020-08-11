#!/usr/bin/env python
# coding: utf-8

# # Transformation of Raw Finance Data for Forecast.
#
# * Takes in raw data from a specified location.
#
# * Transforms Data.
#
# * Outputs.


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
import sys


raw_path = (
    r"S:\Data\Stores Payroll\FY21\05_Forecasts_FY21\Finance Files to Transform\raw_data"
)


print(
    "Please select the file you would like to use by selecting the number listed next to the file."
)


files_, numbers_ = [], []


for num, file in enumerate(Path(raw_path).glob("*.xlsx"), start=1):
    print(num, ":", file.name)
    numbers_.append(num)
    files_.append(file)


files = [x for x in Path(raw_path).glob("**/*") if x.is_file()]


nums_ = [i for i in range(1, len(files) + 1)]


while True:
    cmd = input(f"Select your number between {min(nums_)} - {max(nums_)} :")
    try:
        if int(cmd) in nums_:
            break
    except ValueError:
        print(f"Please select a number between {min(nums_)} - {max(nums_)}")
    else:
        print(f"Please select a number between {min(nums_)} - {max(nums_)}")


file_dict = dict(zip(numbers_, files))


xl = pd.ExcelFile(file_dict[int(cmd)])


print(f"The sheet names from this file are:\n{xl.sheet_names}")


df = pd.read_excel(xl)


print(f"These are the columns we will unpivot\n{df.iloc[:,:3].columns.tolist()}")


print(
    f"These are the week columns with the relevant data\n{df.iloc[:,3:].columns.tolist()[0]} to {df.iloc[:,3:].columns.tolist()[-1]} "
)


print("is this correct?")


while True:
    cmd_2 = input("Please Select [Y] \ [N] : ")
    if cmd_2.lower().strip() == "y":
        break
    elif cmd_2.lower().strip() == "n":
        print(
            "Exiting Program, please correct your data and proceed. the first \nthree columns should be Type, Store & PG"
        )
        sys.exit()
    else:
        print("Please Select Y or N")


# melted_columns
ml_c = df.iloc[:, :3].columns.tolist()


final_ = pd.melt(
    df, id_vars=[ml_c[0], ml_c[1], ml_c[2]], var_name="Week", value_name="Volumes"
)


final_["Week"] = final_["Week"].astype(str).str[-3:]


#


final_["Week"] = final_["Week"].astype(int)


#


week_selector = []

for i in range(1, 54):
    week_selector.append(i)


#


while True:
    cmd_3 = input("Select a minimum start between 1-53: ")
    try:
        if int(cmd_3) in week_selector:
            break
    except ValueError:
        print("Select a valid week.")
    else:
        print("Select a valid week.")

final_ = final_.loc[final_["Week"] >= int(cmd_3)]


#


int(cmd_3)


final_[["Week", "Store", "PG", "Volumes"]].fillna(0)

