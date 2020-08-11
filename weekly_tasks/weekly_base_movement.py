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


# # Weekly Base Movement
# 
# * Change of Process for FY21
# 
# * Use Colleauge File To Calc Base.
# 
# * Get the budgets from the structure Tab.
# 
# * devise a clever method so that we don't 
# <ul>
# 
#   <li>a) duplicate data</li>
#    <li> b) overwrite data</li>
#     <li>c) have a contigancy in place</li>
# 
#     </ul>



# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql("SELECT Shop as store, Base from structure_tab", engine)




file_name,week_,day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")




base_data = r'S:\Data\Stores Payroll\FY21\02_Weekly Tasks\over & underBase Analysis\raw_data'




col_ = pd.read_excel(newest(base_data),skiprows=1)




# Select Retail Shops.

col_ = col_.loc[col_['Location Ledger Code'].isin(structure['store'])].copy()




# Select Perm Only.

col_ = col_.loc[col_['Contract Type'] == 'Permanent']




col_['status'] = np.where(col_['Normal Weekly Hours'] < 38.75, 'PT','FT')




col_ = col_.groupby(["Location Ledger Code", "status"]).agg(
    {"status": "count", "Normal Weekly Hours": "sum"}
).unstack("status").reset_index()




col_.columns = col_.columns.droplevel(0)




col_.columns = ['store','FT', 'PT','FT Hours','PT Hours']




col_['Hours'] = col_['FT Hours'] + col_['PT Hours'] # Calculate base hours.

col_['Heads'] = col_['FT'] + col_['PT'] # Calculate base hours.




df = pd.merge(col_,structure,on='store',how='left')

df['Week'] = week_

df['Year'] = 21

current_cols = pd.read_sql(f"SELECT TOP 1 * from weeklybasemovement",engine).columns.tolist()

df = df[['store','Heads','FT','PT','Base','Hours','Week','Year']]

df.columns = current_cols




if len(pd.read_sql(f"SELECT TOP 1 * from weeklybasemovement where week = {week_} and year = 21",engine)) > 0:
    print(f"Week {week_} already exists in SQL, please check the data, ending the program here to stop any duplicates")
else:
    print(f"Adding {week_} to SQL for FY21")
    df.to_sql("weeklyBaseMovement",engine,schema='dbo',if_exists='append',index=False,dtype=data_types)




data_types = {'Location' : sa.types.VARCHAR(length=50)}




df = pd.read_sql("SELECT * From [weeklyBaseMovement]",engine)
df['Position'] = df['Base'] - df['Hours']
df['% Variance'] = (df['Base'] - df['Hours']) / df['Base']
x = df['% Variance']

conds = [ x <= -0.01, x >= 0.01]
choices = ['overBase',"underBase"]
df['Status'] = np.select(conds,choices,default='atBase')
df.sort_values(['Year','Week','Store'],inplace=True)

s=df.groupby('Store')['Status'].apply(lambda x : x.ne(x.shift()).ne(0).cumsum())
df['Count']=df.groupby([df.Store,s]).cumcount()+1


df['Position'] = df['Base'] - df['Hours']
df['% Variance'] = (df['Base'] - df['Hours']) / df['Base']

x = df['% Variance']

conds = [ x <= -0.01, x >= 0.01]

choices = ['overBase',"underBase"]

df['Status'] = np.select(conds,choices,default='atBase')

df.sort_values(['Year','Week','Store'],inplace=True)

s=df.groupby('Store')['Status'].apply(lambda x : x.ne(x.shift()).ne(0).cumsum())

df['Count']=df.groupby([df.Store,s]).cumcount()+1

a = df.loc[(df.Week == int(week_-1)) & (df.Year == 21)]





os.chdir(r'S:\Data\Stores Payroll\FY21\02_Weekly Tasks\over & underBase Analysis\outputs')




writer = pd.ExcelWriter(f"{file_name}_over_&_under_base_analysis.xlsx",)
df.to_excel(writer,'Raw_Data',index=False)
a.to_excel(writer,'Current Week',index=False)
writer.save()
writer.close()




for file in Path(base_data).glob('*.xlsx'):
    file.rename(Path(file.parent, f"{file_name}_{file.stem}{file.suffix}"))




for file in Path(base_data).glob('*.xlsx'):
    shutil.move(str(file), os.path.join(str(file.parent) + '\\processed', str(file).split('\\')[-1]))

















