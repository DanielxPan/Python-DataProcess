# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 14:45:36 2023

@author: danielp
"""

import pandas as pd
import datetime as dt
# from datetime import timedelta

# Step0: Print date text

today = dt.date.today()
# for test today = today - dt.timedelta(days=1)
# for test today_str = str(today)
# for test today_str='2023-10-29'
# print(today_str)
print(today)

#1. Create date list of last 7 days
# Initialize a list to store the dates
list_date = []

for i in range(1, 8):
# for test for i in range(5, 7):
    # Calculate the date by subtracting days from the current date
    previous_date = today - dt.timedelta(days=i)
    # Append the previous date to the list
    list_date.append(previous_date.strftime('%Y-%m-%d'))

# Common used variables
fmt_csv = '.csv'
fmt_xlsx = '.xlsx'

### Folder variables
path_result_file = r"C:\Users\Reddrop\Downloads\\"
# Changed to find in local downloads folder, because now move results files every Sunday 27AUG24'
# path_result_file = r"C:\data\Lightyear\06.ScheduledAutomation\98.ResultsFiles\\"
path_ref_table = r"C:\data\Lightyear\06.ScheduledAutomation\01.RawData_ReferenceTable\Ref_StoreNameCode.csv"
path_err_files = r"C:\data\Lightyear\06.ScheduledAutomation\03.ErrorMessageFiles\\"

# Step1: Read Files

import os

list_file = []
# Iterate over the files in the directory
for filename in os.listdir(path_result_file):
    # Check if the filename starts with any date string from the list
    for date_string in list_date:
        if filename.startswith(date_string):
            list_file.append(filename)
            break # Move to the next file once a match is found.
                  # Break from the 2nd loop and go back to the first loop

# Step2- Append into 1 df
# Create a list to append dataframes
df_result_all=[]
for i in list_file:
#for i in range(0,2):
    path_result_csv = path_result_file + "\\" + i
    print(path_result_csv)
    df_result = pd.read_csv(path_result_csv)
    df_result['FileName'] = i
    df_result_all.append(df_result)

df_result_all = pd.concat(df_result_all)

df_result_all = df_result_all.drop_duplicates()

# Check data sample
df_reslt_smpl = df_result_all.head(10)
df_reslt_smpl.dtypes

# Filter failed uploading lines
fil_fail_result = df_result_all['Success/Fail'] == 'Fail'
df_result_failed = df_result_all[fil_fail_result]
df_result_failed.dtypes

# Create list of columns and filter main columns
list_col = df_result_all.columns.tolist()
fil_col = [
            'StoreCodes'
           ,'VENDORID'
           ,'VENDNAME'
           ,'Supplier'
           ,'Product Code'
           ,'Current Price'
           ,'Success/Fail'
           ,'Reason'
           ,'FileName'
           ]

df_result_failed = df_result_failed[fil_col].drop_duplicates()
df_result_failed.dtypes
# Create list of successful lines
df_result_sus = df_result_all[ ~fil_fail_result ]

# Join back the success product upload
df_result_failed = pd.merge (
                            df_result_failed,
                            df_result_sus,
                            how = 'left',
                            on = ['StoreCodes'
                                   ,'Supplier'
                                   ,'Product Code'],
                            indicator=True,
                            suffixes=('_failed', '_success')
                            )

# Filter out only the left part
df_result_failed = df_result_failed[df_result_failed['_merge']=='left_only']

# Filter necessary columns again
fil_col = [
            'StoreCodes'
           ,'VENDORID_failed'
           ,'VENDNAME_failed'
           ,'Supplier'
           ,'Product Code'
           ,'Current Price_failed'
           ,'Success/Fail_failed'
           ,'Reason_failed'
           ,'FileName_failed'
           ]

df_result_failed = df_result_failed[fil_col].drop_duplicates()

# In case the job runs overnight
today = dt.date.today()
# for test today = today - dt.timedelta(days=0)
# for test today_str = str(today)
# for test today_str='2023-10-29'
# print(today_str)
print(today)

# Save result to csv
outpath_failed_files = path_err_files + '\\' + str(today)  + '_Upload_Failed_Store_Supplier_File.csv' 
print(outpath_failed_files)
df_result_failed.to_csv(outpath_failed_files,sep=',',index=False,header=True)
