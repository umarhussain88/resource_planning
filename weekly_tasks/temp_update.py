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




# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql("SELECT Shop as store, Base from structure_tab", engine)




file_name,week_,day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


# # Temporary Colleague Calculator Update.




"""
1. Read in Latest Base Data.
2. read in latest colleague data data.
3. do some basic maniuplation and create a dataframe with one entry per col for each
week in the year. (so 1 * 53)
4. Read in the Current SQL table.
5. Merge the Input, cDate, and Verification with a left join (this removes leavers and adds in starters)
6. for new starters, find these and change these to null values for our SQL dB.

"""




col_data = r'S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Temps\raw_data'




col_ = pd.read_excel(newest(col_data),skiprows=1)




col_.rename(columns={'Location Ledger Code' : 'store'},inplace=True)

col_['Full Name'] = col_['First Name'] + ' ' + col_['Last Name']

col_= col_[['Full Name','Number','Normal Weekly Hours','store']].copy()

col_ = col_.loc[col_['store'].isin(structure['store'])]




col_['Week'] = 1

col_ = pd.concat([col_]*52)

col_.Week = col_.Week.add(col_.groupby('Number').cumcount())




inputs = pd.read_sql("SELECT * from colleague_data",engine)





new_cols = pd.merge(col_,inputs[['Number','cDate','Verification','Input','Week']],on=['Number','Week'],how='left')





last_week = inputs['Number'].nunique()
today_c = new_cols['Number'].nunique()





print(f"WoW we have seen a reduction of {last_week - today_c} colleagues")






new_cols = new_cols[['Full Name', 'Number', 'Normal Weekly Hours', 'store', 'Input', 'Week',
       'cDate', 'Verification']]






"""
as Available Hours changes WoW due to extra hours,
and the base is updated to due to colleauges leaving and starting, we will write this into SQL each weeek.

Currently writing this in here.

"""






ah = pd.read_sql('SELECT * from available_hours',engine)


xh = pd.read_sql("SELECT Hours, Store, Week from extraHoursDetails",engine)


holsB = pd.read_sql("SELECT * from holiday_budget",engine)


ah = pd.melt(ah,id_vars='Store',var_name='Week',value_name='AH Hours')


ah.Week = ah.Week.astype(int)


xh = xh.groupby(['Store','Week'])['Hours'].sum().reset_index()


hours = pd.merge(ah,xh,on=['Store','Week'],how='left')


base = pd.read_sql('SELECT * from weeklyBaseMovement where Year = 21',engine)



hours.Store = hours.Store.astype(int)
hours['Week'] = hours['Week'] - 2100


hours = pd.merge(hours,base[['Store','Week','Base']],on=['Week','Store'],how='left')
hours = hours.sort_values(['Store','Week'])


hours['Base'] = hours['Base'].ffill()
hours.Hours = hours['AH Hours'] + hours['Hours'].fillna(0)



hours = hours.drop('AH Hours',axis=1)

holsB = pd.melt(holsB,id_vars='Store',var_name='Week',value_name='Holiday Budget')
holsB['Week'] = holsB['Week'].astype(int) - 2000

holsB['Store'] = holsB['Store'].astype(int)
hours = pd.merge(hours,holsB, on=['Store','Week'],how='left')

hours.rename(columns={'Holiday Budget' : 'HolsB'},inplace=True)
hours = hours.fillna(0)


os.chdir(r'S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Temps\sql_extract')

hours.to_csv(f'{file_name}_temp_poc.csv.gz',index=False,compression='gzip')


new_cols.to_csv(f'0{file_name}_colleague_data.csv.gz',index=False,compression='gzip')



hours.rename(columns={'Holiday Budget' : 'HolsB'},inplace=True)

types = {'Store' : sa.types.BIGINT,
        'Week' : sa.types.BIGINT,
        'Hours' : sa.types.FLOAT,
        'Base' : sa.types.FLOAT,
         'HolsB' : sa.types.FLOAT}

hours.to_sql('tempPOC',engine,schema='dbo',if_exists='replace',index=False,dtype=types)
print("Complete")


types_2 = {'Full Name' : sa.types.VARCHAR(length=50),
        'Number' : sa.types.BIGINT,
        'Normal Weekly Hours' : sa.types.FLOAT,
        'Shop' : sa.types.BIGINT,
         'Input' : sa.types.FLOAT,
         'Week' : sa.types.BIGINT,
         'cDate' : sa.types.NVARCHAR(length=255),
         'Verification' : sa.types.VARCHAR(length=50)}



new_cols.to_sql('ColData',engine,schema='dbo',if_exists='replace',index=False,dtype=types_2)
print("Complete")





for file in Path(col_data).glob('*.xlsx'):
    file.rename(Path(file.parent, f"{file_name}_{file.stem}{file.suffix}"))




for file in Path(col_data).glob('*.xlsx'):
    shutil.move(str(file), os.path.join(str(file.parent) + '\\processed', str(file).split('\\')[-1]))
    
print("Colleague Data moved into processed.")















