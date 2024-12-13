# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import datetime as dt

# Step0: Print date text


today=dt.date.today()
today_str = str(today)
# for test today_str='2023-10-23'
print(today_str)

# Common used variables
fmt_csv = '.csv'
fmt_xlsx = '.xlsx'
path_vdr_list = r"C:\\\data\Lightyear\0.VendorsList\\"
path_ref_table = r"C:\\\data\Lightyear\06.ScheduledAutomation\01.RawData_ReferenceTable\\"

# Step1: Read Files
file_vdr_list = path_vdr_list + "\\00.SageVendorList\\" + "Vendors (Update " + today_str +  ").xlsx"
print(file_vdr_list)

df_vdr = pd.read_excel(file_vdr_list)

# Step2: Filter new added & inactive suppliers based on the conditions

# Step2-1: Filter Columns
col_sel1 = ['VENDORID','VENDNAME','IDGRP','SWACTV','DATELASTMN','DATELASTIV']
df_vdr = df_vdr[col_sel1]

df_vdr['DATELASTMN'] = df_vdr['DATELASTMN'].dt.date
df_vdr['DATELASTIV'] = df_vdr['DATELASTIV'].dt.date
df_vdr['DATE_LAST_Update'] = today

# Step2-2: Create new columns
df_vdr['StoreCodes'] = df_vdr['IDGRP'].str[:3]
df_vdr['GRP'] = df_vdr['IDGRP'].apply(lambda x: x[3:len(x)])

### Added on 8thJuly, filter only active store
file_store_list = path_ref_table + "Ref_StoreNameCode.csv"
print(file_store_list)

df_store = pd.read_csv(file_store_list, low_memory = False, encoding='latin-1')

# Create active store code
list_act_store_code = df_store['StoreCodes'].unique().tolist()

# Filter df_vdr by store code
df_vdr = df_vdr[df_vdr['StoreCodes'].isin(list_act_store_code)]
### Added on 8thJuly, filter only active store

# Step2-3: Set up filters
#filter 1.Column F within last 7 days
last_week = today - dt.timedelta(days=7)

fil1 = ( df_vdr['DATELASTMN'] >  last_week) & (df_vdr['DATELASTMN'] <= today)

#filter 2.VendorID is not in the list
file_sup_list = path_ref_table + "StoreSuppliersReferenceTable.csv"
print(file_sup_list)

df_cur_vdr = pd.read_csv(file_sup_list, low_memory = False, encoding='latin-1')

df_cur_vdr = df_cur_vdr.dropna(subset=['VENDORID'])

### Check latest update date
# df_cur_vdr.dtypes
# df_cur_vdr['DATE_LAST_Update'] = pd.to_datetime(df_cur_vdr['DATE_LAST_Update'] 
#                                                 ,format='%d/%m/%Y')

# df_cur_vdr['DATE_LAST_Update'].max()
### Check latest update date

cur_vdr_list = df_cur_vdr['VENDORID'].unique().tolist()
# Add ~ to reverse true to false
fil2 = ~df_vdr['VENDORID'].isin(cur_vdr_list)

#filter 3.Column C contains INV / MET
fil3 = (df_vdr['GRP'] == 'INV') | (df_vdr['GRP'] == 'MET')

#filter 4.Active
fil4 = df_vdr['SWACTV'] == 1

#filter 5.Inactive
fil5 = df_vdr['SWACTV'] == 0

#filter 6. Use DATELASTIV to fitler Last 12 months OR blank, add on 31OCT
no_invoice_date_condition = df_vdr['DATELASTIV'].isna()
last_year = today - dt.timedelta(days=365)
fil6 = (df_vdr['DATELASTIV'] >= last_year) | (no_invoice_date_condition)

# Filter new added suppliers
df_new_vdr = df_vdr[fil1 & fil2 & fil3 & fil4 & fil6]

# Filter inactive suppliers
df_inact_vdr = df_vdr[fil5]

# Step3: Remove inactive supplier from the existing supplier lists

# Step3-1: Filter deactivated suppliers based on the conditions
inact_vdr_list = df_inact_vdr['VENDORID'].unique().tolist()
fil7 = ~df_cur_vdr['VENDORID'].isin(inact_vdr_list)

df_cur_act_vdr = df_cur_vdr[fil7]

# Step4: Append new added suppliers to the existing supplier lists

# Step4-1: Create Reference Tables
# Current Reference Table
df_cur_act_vdr_ref = df_cur_act_vdr[['VENDORID(4Digits)','SupplierID']].drop_duplicates().dropna()

# Reference Table from Last week
file_ref_list_last_wk = path_vdr_list + "\\02.SupplierNotMatched\\" + str(last_week) + "_Supplier_NotMatched" + fmt_csv
print(file_ref_list_last_wk)

df_vdr_ref_last_wk = pd.read_csv(file_ref_list_last_wk, low_memory = False, encoding='latin-1')

df_vdr_ref_last_wk = df_vdr_ref_last_wk[['VENDORID(4Digits)','SupplierID']].drop_duplicates().dropna()

# Use join instead of append
df_cur_act_vdr_ref_all = df_cur_act_vdr_ref.append(df_vdr_ref_last_wk).drop_duplicates().dropna()

# Step4-2: Add missing supplier ID
df_cur_act_mis_vdr = df_cur_act_vdr[df_cur_act_vdr['SupplierID'].isna()]
df_cur_act_vdr = df_cur_act_vdr[~df_cur_act_vdr['SupplierID'].isna()]

# Add supplier ID
df_cur_act_mis_vdr = pd.merge(df_cur_act_mis_vdr
                             ,df_cur_act_vdr_ref_all
                             ,on='VENDORID(4Digits)'
                             , how='left')

# Coalesce 2 columns
df_cur_act_mis_vdr['SupplierID'] = df_cur_act_mis_vdr.SupplierID_x.combine_first(df_cur_act_mis_vdr.SupplierID_y)

# Filter Columns
col_sel2 = df_cur_act_vdr.columns.values.tolist()
df_cur_act_mis_vdr =  df_cur_act_mis_vdr[col_sel2]

# Append back missing rows
df_cur_act_vdr =  df_cur_act_vdr.append(df_cur_act_mis_vdr).drop_duplicates()

# Step4-2: Create VENDORID(4Digits)
df_new_vdr['VENDORID(4Digits)'] = df_new_vdr['VENDORID'].apply(lambda x: x[3:len(x)])

df_new_vdr = pd.merge(df_new_vdr
                     ,df_cur_act_vdr_ref_all
                     ,on='VENDORID(4Digits)'
                     , how='left')

# Step4-3: Filter columns & append to current vendor list
col_sel2 = df_cur_act_vdr.columns.values.tolist()
df_new_vdr = df_new_vdr[col_sel2]

df_cur_act_new_vdr = df_cur_act_vdr.append(df_new_vdr)

# Step4-3: Filter missing suppliers
check_miss_sup = df_new_vdr[df_new_vdr['SupplierID'].isna()]

df_cur_act_miss_vdr = df_cur_act_vdr[df_cur_act_vdr['SupplierID'].isna()]

check_miss_sup = check_miss_sup.append(df_cur_act_miss_vdr)

# Step4-4: Output csv file if there is any missing suppliers
fil_path_miss_vdr_list = path_vdr_list + "\\02.SupplierNotMatched\\" + today_str + "_Supplier_NotMatched" + fmt_csv 
print(fil_path_miss_vdr_list)

check_miss_sup.to_csv(fil_path_miss_vdr_list,sep=',',index=False,header=True)

# Step4-5: Output csv file with suppliers
fil_path_vdr_sup_list = path_vdr_list + "\\03.NewSuppliers\\"  + today_str + "_New_Suppliers" + fmt_csv 
print(fil_path_vdr_sup_list)

df_new_vdr.dropna().to_csv(fil_path_vdr_sup_list,sep=',',index=False,header=True)

# Step6: Save Store supplier reference
print(file_sup_list)
df_cur_act_new_vdr.to_csv(file_sup_list,sep=',',index=False,header=True)




