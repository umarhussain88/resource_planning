#!/usr/bin/env python
# coding: utf-8

# # Performance Hours and Sales #
# 
# ## FY21 Change Log##
# 
# As there will no longer a performance calculator, or any visibility of weekly sales budgets and targets from resource planning, we will not need to maintain the data for the front end.
# 
# * Performance Hours will be run weekly but only kept as a running quaterly log for shops.
# * Performance Hours will be displayed in the Deployment Dashboard (TBC) for AM's and DD's to view.
# 
# * The Calcualtions for the Perf are as follows : 
# 
# For Each Category - Auto - Bike - Services : 
# 
# 
# (Sales - Forecast) / 600 - for Auto
# (Sales - Forecast) / 200 - for Bike
# (Sales - Forecast) / 40 -  for Services
# 
# These are then aggregated into a total performance number and the awarded or deducted on the following conditions: 
# 
# * If the Perfomance hours are < 0 but the Total Forecast was hit then no hours area taken.
# * If the Performance Hours are >= 0 then these are awarded to the shop. 
# 
# 
# 
# 



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
import re
import numpy as np




# fy21 dates.

dates = pd.read_sql("SELECT * from fy21_calendar", con=engine)

# structure tab wtih shop names etc.

structure = pd.read_sql("SELECT Shop as store, Base from structure_tab", engine)


# # Datetime



file_name,week_,day_ = halfords_week(dates)
print(f"We are {week_} weeks away from FY21")


# # Get Budgets.



sales_budgets = ['auto_budgets','bike_budgets','service_budgets']

# read, melt and merge into a tabular format by reading each table into a dict.


data = {}
# could do this as a liner with list-comp but this is more readable imo. 

for category in sales_budgets:
    df = pd.read_sql(f"SELECT * from {category}",engine) 
    df_melt = pd.melt(df,id_vars='Store',var_name='Week',value_name=f'{category}') 
    data[f'{category}'] = df_melt


# merge.     
df = pd.merge(data['auto_budgets'],data['service_budgets'],on=['Week','Store'],how='left')

budget = pd.merge(df,data['bike_budgets'],on=['Week','Store'],how='left')

budget['Total'] = budget.iloc[:,2:].sum(axis=1) # row wise sum. 

budget = pd.melt(budget,id_vars=['Store','Week'],var_name='Category',value_name='Budget')

d_ = ['Auto','Bikes','Services']
di_ = dict(zip(sales_budgets,d_))


budget['Category'] = budget['Category'].map(di_).fillna('Total') # Map Budgets to match Actuals Name for Merge. 

budget['Week'] = budget['Week'].astype(int)


# # Get Actuals



raw_data = r'S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Actual Sales - Performance Hours\raw_sales_data'





actuals = pd.read_excel(newest(raw_data),sheet_name='Table',skiprows=15).iloc[:-1,6:].drop('Unnamed: 7',axis=1)
actuals_week = pd.read_excel(newest(raw_data),sheet_name='Table',skiprows=14).iloc[:,8:9].columns.tolist()
actuals_year =  re.findall(r'(\d+)',actuals_week[0])[1]
actuals_week = re.findall(r'(^\d+)',actuals_week[0])
actuals.rename(columns = {'Site' : 'Store'},inplace=True)





unique_stores = actuals['Store'].nunique()





print(
f"""The week listed in this file is {int(actuals_week[0])} and the year listed is {actuals_year}
there are {unique_stores} stores in this file"""
)










actuals['Week'] = int(actuals_week[0]) + 2100





actuals_ = pd.melt(actuals,id_vars=['Store','Week'],var_name='Category',value_name='Actual')





perf = pd.merge(actuals_,budget,on=['Store','Week','Category'],how='inner')


# # Get Min & Variable Perf.




min_perf = pd.read_sql("SELECT * FROM min_perf",engine)





sql_week = int(actuals_week[0]) + 2100





print(f"Reading Week {sql_week} for Variable Hours")





variable = pd.read_sql(f"SELECT Store, {[sql_week]} as variable from variable_hours",engine)





finalPerf = perf.loc[perf['Category'] == 'Total'].copy()





perf_lookups = pd.merge(min_perf,variable,on='Store')





finalPerf = pd.merge(finalPerf,perf_lookups,on='Store')







# # Calculate Perf.




# Workout Individual Perf.

perf = perf.loc[perf['Category'] != 'Total'].copy()

perf.loc[perf.Category == 'Bikes','Perf'] = (perf['Actual'] - perf['Budget'])/200
perf.loc[perf.Category == 'Services','Perf'] = (perf['Actual'] - perf['Budget'])/40
perf.loc[perf.Category == 'Auto','Perf'] = (perf['Actual'] - perf['Budget'])/600





# Create a grouped dataframe with the totals.



finalPerf = pd.merge(
    finalPerf,
    perf.groupby(["Store"])["Perf"].sum().reset_index(),
    on=["Store"],
    how="left",
)





conditions = [
    (finalPerf["Actual"] > finalPerf["Budget"]) & (finalPerf["Perf"] < 0),
    (finalPerf["Perf"] > 0) & (finalPerf["Budget"] > finalPerf["Actual"]),
    (finalPerf["Perf"] >= 0),
    (finalPerf["Perf"] < finalPerf["variable"])
    & (finalPerf["variable"] > finalPerf["min_perf"]),
    (finalPerf["min_perf"] > finalPerf["Perf"])
    & (finalPerf["min_perf"] > finalPerf["variable"]),
    (finalPerf["Perf"] > finalPerf["min_perf"])
    & (finalPerf["Perf"] > finalPerf["variable"]),
]


outputs = [
    0,
    finalPerf["Perf"],
    finalPerf["Perf"],
    finalPerf["variable"],
    finalPerf["min_perf"],
    finalPerf["Perf"],
]





finalPerf['Actual Perf'] = np.select(conditions,outputs,default=finalPerf['Perf'])


finalPerf['Actual Perf'] = np.ceil(finalPerf['Actual Perf']*4)/4


# # Save output.




os.chdir(r'S:\Data\Stores Payroll\FY21\02_Weekly Tasks\Actual Sales - Performance Hours\outputs')






writer = pd.ExcelWriter(file_name + 'Performance_Hours.xlsx')
finalPerf.to_excel(writer,'Total_Perf',index=False)
perf.to_excel(writer,'Performance_Category',index=False)
writer.save()


# # I/O O




for file in Path(raw_data).glob('*.xlsm'):
    file.rename(Path(file.parent, f"{file_name}_{file.stem}{file.suffix}"))





for file in Path(raw_data).glob('*.xlsm'):
    shutil.move(str(file), os.path.join(str(file.parent) + '\\processed', str(file).split('\\')[-1]))






