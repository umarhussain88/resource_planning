#!/usr/bin/env python
# coding: utf-8


import sys,os
from pathlib import Path
from time import sleep

sys.path.append(
    os.path.abspath(
        r"S:\Data\Stores Payroll\FY21\99_Master Scripts (DO NOT EDIT)\dB_Connector"))

from connector import *

sys.path.append(
    os.path.abspath(
        r"S:\Data\Stores Payroll\FY21\99_Master Scripts (DO NOT EDIT)\common_functions"))

from halfords_functions import newest, halfords_week

import pandas as pd
from datetime import datetime
import shutil
import numpy as np
import glob

# Modified version of Newest - couldn't figure this out in Pathlib (in a readable format)


def newest(path,pattern):
    os.chdir(path)
    files = glob.glob(f"*{pattern}*.*xlsx")
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)


# # Contract Errors FY21


# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql("SELECT Shop as Store, Area from structure_tab", engine)



file_name,week_,day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")



path = r'S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Contractual Errors\raw_data'




bank = pd.read_excel(newest(path,'bank'),skiprows=1)
hours = pd.read_excel(newest(path,'hours'),skiprows=1)
contract = pd.read_excel(newest(path,'contract'),skiprows=1)


# # Bank Details




bank = bank[['First Name','Number','Last Name','Location','Seniority Date']].copy()
bank['Missing Bank Details'] = 'True'
bank['Store'] = bank['Location'].str.extract('(\d+)').astype(int)
bank['ErrorType'] = 'Missing Bank Details'

bank = bank[['First Name','Last Name','Number','Store','Seniority Date','Missing Bank Details','ErrorType']].copy()
bank.rename(columns={'Seniority Date' : 'Effective Start','Number' : 'Employee Number'},inplace=True)

b1 = bank[['First Name','Last Name','Employee Number','Store','Effective Start','ErrorType']].copy()


# # Contract Errors 




contract['Missing Work Contract'] = 'True'
contract['Store'] = contract['Org Unit'].str.extract('(\d+)').fillna(0).astype(int)
contract['ErrorType'] = 'Contract Error'
contract = contract[['First Name','Last Name','Employee Number','Store','Effective Start','Missing Work Contract','ErrorType']].copy()

c1 = contract[['First Name','Last Name','Employee Number','Store','Effective Start','ErrorType']].copy()


# # Hours Check



hours = pd.read_excel(newest(path,"Hours"),skiprows=1,date_parser='Effective Start')

hours.loc[hours['Work Pattern Start Day'] != 'Saturday','Saturday'] = "True" 


hours.loc[hours['Work Contract Hours'] != (hours['Normal Weekly Hours']*2),'Work Contract Hours not Bi-Weekly'] = "True" 


hours.loc[hours['Work Contract Hours'] != hours['Work Pattern Hours'],'No Matching Work Pattern'] = "True"

hours.loc[hours['Work Contract'] == 'Weekly','No Matching Work Pattern'] = "True"

hours['Store'] = hours['Location'].str.extract('(\d+)').fillna(0).astype(int)


hours['ErrorType'] = 'Missing Work Pattern'


hours = (hours[['First Name','Last Name','Employee Number','Store','Effective Start','Saturday',
                
                'Work Contract Hours not Bi-Weekly','No Matching Work Pattern','ErrorType']].copy())


h1 = hours[['First Name','Last Name','Employee Number','Store','Effective Start','ErrorType']]


# # Create SQL Dataframe



SQLdf = pd.concat([h1,c1,b1],ignore_index=True)




SQLdf = pd.merge (structure,SQLdf,on='Store',how='right')




SQLdf = SQLdf.fillna(0)




SQLdf['Week'] = week_




SQLdf = SQLdf[
    [
        "Area",
        "Store",
        "Employee Number",
        "First Name",
        "Last Name",
        "Effective Start",
        "Week",
        "ErrorType",
    ]
].copy()

columns_ct = ['Area', 'Shop Number', 'Employee Number', 'First Name', 'Last Name',
       'Effective Start', 'Week', 'Error Type']

SQLdf.columns = columns_ct


# ## set metatypes



SQLdf.dtypes




data_types = {'First Name' : sa.types.VARCHAR,
             'Last Name' : sa.types.VARCHAR,
             'Effective Start' : sa.types.VARCHAR,
             'Error Type' : sa.types.VARCHAR}




max_week = pd.read_sql("SELECT max(Week) as W from contract_errors_ytd",engine)['W'][0]








while True:
    print(f"Do you use to append to the YTD table? the current week is {week_-1} and the max week in SQL is {max_week}")
    cmd = input("Enter [Y] or [N]")
    if cmd.lower().strip() == 'y':
        SQLdf.to_sql('contract_errors_ytd',engine,schema='dbo',if_exists='append',index=False,dtype=data_types)
        print("YTD updated to SQL")
        break
    elif cmd.lower().strip() == "n":
        print("Not updating YTD updating weekly table")
        break
    else:
        print("Enter either yes/no")






# # Create Intranet Table



hours = hours.iloc[:,:-1]
contract = contract.iloc[:,:-1]
bank = bank.iloc[:,:-1]




df = SQLdf.iloc[:,:-2]



df = pd.merge(
    df,
    hours[
        [
            "Employee Number",
            "Saturday",
            "Work Contract Hours not Bi-Weekly",
            "No Matching Work Pattern",
        ]
    ],
    on="Employee Number",
    how="left",
).copy()




df = pd.merge(df, contract[["Employee Number", "Missing Work Contract"]], how="left").copy()




df = pd.merge(df, bank[["Employee Number", "Missing Bank Details"]], how="left").copy()




df.iloc[:,-5:] = df.iloc[:,-5:].fillna(" ")




df['Effective Start'] = pd.to_datetime(df['Effective Start'],dayfirst=True,errors='coerce')




df = df.drop_duplicates(subset='Employee Number',keep='last')




df = df.loc[(df.Area.isnull() == False)]

df.rename(columns={"Shop Number": "Shop"}, inplace=True)
df.columns = [
    "Area",
    "Shop",
    "Number",
    "First Name",
    "Last Name",
    "Effective Start",
    "Saturday",
    "biWeekly",
    "noMatching",
    "missingCont",
    "missingBank",
]




d = {
    "Area": sa.types.BIGINT,
    "Shop": sa.types.BIGINT,
    "Employee Number": sa.types.BIGINT,
    "First Name": sa.types.NVARCHAR(length=50),
    "Last Name": sa.types.NVARCHAR(length=50),
    "Effective Start": sa.types.NVARCHAR(length=50),
    "Saturday": sa.types.NVARCHAR(length=50),
    "biWeekly": sa.types.NVARCHAR(length=50),
    "noMatching": sa.types.NVARCHAR(length=50),
    "missingCont": sa.types.NVARCHAR(length=50),
    "missingBank": sa.types.NVARCHAR(length=50),
}

df = df.replace(" ", np.nan)




df.to_sql("contractErrorsWeek",engine,schema='dbo',index=False,if_exists='replace',dtype=d)
print("WTD Updated, Time Updated")
import datetime
last_updated = datetime.date.today().strftime("%A %d %B")
lu = pd.DataFrame({'Today' : last_updated},index=[0])
tDtypes = {'Today' : sa.types.VARCHAR(length=255)}
lu.to_sql('CE_Update',engine,dtype=tDtypes,index=False,schema='dbo',if_exists='replace')




for file in Path(path).glob('*.xlsx'):
    file.rename(Path(file.parent, f"{file_name}_{file.stem}{file.suffix}"))




for file in Path(path).glob('*.xlsx'):
    shutil.move(str(file), os.path.join(str(file.parent) + '\\processed', str(file).split('\\')[-1]))





