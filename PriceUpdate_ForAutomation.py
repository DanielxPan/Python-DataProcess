# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import datetime

# Common used variables
fmt_csv = '.csv'
fmt_xlsx = '.xlsx'
path_vdr_list = r"C:\\\data\Lightyear\0.VendorsList\\"
path_ref_table = r"C:\\\data\Lightyear\06.ScheduledAutomation\01.RawData_ReferenceTable\\"
path_err_files = r"C:\\\data\Lightyear\06.ScheduledAutomation\03.ErrorMessageFiles"

# Step1: Read CSV Files
# Import store suppliers lists
#csv_file1 = r"C:\\\data\Lightyear\06.ScheduledAutomation\01.RawData_ReferenceTable\StoreSuppliersLists.csv"
csv_file1 = path_ref_table + "StoreSuppliersReferenceTable.csv"
print(csv_file1)

df_suppliers = pd.read_csv(csv_file1, low_memory = False, encoding='latin-1')
df_suppliers = df_suppliers.dropna(subset=['SupplierID'])
df_suppliers = df_suppliers.rename(columns={'SupplierID': 'Supplier'})

# Import Update Products Lists
today=datetime.date.today()
today=str(today)
# for testtoday='2023-10-25'
print(today)
#csv_file2 = 'PriceUpdate_' + today +'.csv'
#df_prc_list = pd.read_csv(csv_file2, low_memory = False, encoding='latin-1')
csv_file2 = r"C:\\\data\Lightyear\06.ScheduledAutomation\01.RawData_ReferenceTable" + '\PriceUpdate_' + today + '.xlsx'
df_prc_list = pd.read_excel(csv_file2)
df_prc_list = df_prc_list.rename(columns={'Supplier Code': 'Supplier'})
df_prc_list = df_prc_list.dropna(how='all')

# Filter columns to keep (exclude columns starting with 'Unnamed') add on 26OCT
columns_to_keep = [col for col in df_prc_list.columns if not col.startswith('Unnamed')]

# Create a new DataFrame with only the selected columns
df_prc_list = df_prc_list[columns_to_keep]

#Missing Suppliers
#fil_sup1 = ['HOL','RIF']
#fil_sup2 = df_prc_list['Supplier'].isin(fil_sup1)
#df_prc_list = df_prc_list[fil_sup2]

#Check na
check1 = df_prc_list[df_prc_list['Supplier'].isna()]

# Import Department Code Ref
csv_file3 = r"C:\\\data\Lightyear\06.ScheduledAutomation\01.RawData_ReferenceTable\DepartmentCodeRef.csv"
df_dep = pd.read_csv(csv_file3, low_memory = False, encoding='latin-1')

# Step2: Join tables
# Step2-1: Join Department Ref
df_sup_prc = pd.merge(df_prc_list
                      ,df_dep
                      ,on='Department'
                      , how='left')

# Check All Supplier Lists are in product list
check2 = df_sup_prc.Supplier.unique().tolist()

# Step2-2: Join Supplier List on SupplierID
df_str_sup_prc = pd.merge(df_sup_prc
                          ,df_suppliers
                          ,on='Supplier'
                          , how='left')

#Check not in Supplier Reference
check3 = df_str_sup_prc[df_str_sup_prc['StoreCodes'].isna()]
#Out put csv file if there is any missing stores
outpath0=r"C:\\\data\Lightyear\06.ScheduledAutomation\03.ErrorMessageFiles" + "\\" + today + "_Missing_Supplier_File" + '.csv'
print(outpath0)
check3.to_csv(outpath0,sep=',',index=False,header=True)

# Check All Supplier Lists are in store/supplier/product list
check4 = df_str_sup_prc.Supplier.unique().tolist()

# Create the new column "Default GL Code" based on the "Department" values added on 8Nov per Tammy's request
df_str_sup_prc['Default GL Code'] = df_str_sup_prc.apply(
                                                        lambda row: f"{row['StoreCodes']}-5250-{row['DepartmentCode']}"
                                                        #lambda row: is a lambda function that takes a row as input. Each row in the DataFrame is passed to this function one by one
                                                        if row['Department'] == 301
                                                        else f"{row['StoreCodes']}-5000-{row['DepartmentCode']}",
                                                        axis=1, # axis=1 specifies that we want to apply the function to each row
                                                        )

#Make ALM & DAVD 10% from Tammy's request
df_str_sup_prc.columns
sup_10 = ['ALM','DAVD','METNSW','METIGA']
df_str_sup_prc_10 = df_str_sup_prc[df_str_sup_prc['Supplier'].isin(sup_10)]
df_str_sup_prc_05 = df_str_sup_prc[~df_str_sup_prc['Supplier'].isin(sup_10)]

df_str_sup_prc_10['Tolerance(+/-)'] = 10

df_str_sup_prc_05['Tolerance(+/-)'] = 0.05

df_str_sup_prc= df_str_sup_prc_10.append(df_str_sup_prc_05)

# Step3: Process table

# Step3-1: Filter Columns
col_sel1 = df_str_sup_prc.columns.values.tolist()
fil1 = col_sel1[10:11] + col_sel1[2:3] + col_sel1[-2:-1] + col_sel1[14:15] + col_sel1[-1:] + col_sel1[16:19] + col_sel1[8:9]
df_str_sup_prc = df_str_sup_prc[fil1]

# Step3-2: Rename Columns
df_str_sup_prc = df_str_sup_prc.rename(columns={'Manuf Code': 'Product Code', 'Cost Excl GST': 'Current Price'})
# Round to 2 digits
df_str_sup_prc['Current Price'] = df_str_sup_prc['Current Price'].round(2)
# Add on 15Mar, remove negative price
fil_pos = df_str_sup_prc['Current Price']>=0
df_str_sup_prc = df_str_sup_prc[fil_pos]
# Check no product codes
check5 = df_str_sup_prc[df_str_sup_prc['Product Code'].isna()]

# Step3-3: Drop Product Code = Na
# Drop na https://blog.csdn.net/lwgkzl/article/details/80948548
df_str_sup_prc = df_str_sup_prc.dropna(subset=['Product Code'])

# Step4: Subset df by stores
# 4-1: Extact store lists 
# Formula1: DF.ColumnName.unique().tolist()
stores = df_str_sup_prc.dropna(subset=['StoreCodes'])
stores = stores.StoreCodes.unique().tolist()
print(stores)

# Formula2: pd.unique(DF[ColumnName])
suppliers = pd.unique(df_str_sup_prc['Supplier']).tolist()
print(suppliers)

# 4-2: Filter by store & 4-3: Loop to subset df
# Source: https://stackoverflow.com/questions/50003885/subsetting-a-dataframe-into-individual-dataframes-using-a-loop-python-pandas
dfs = dict(tuple(df_str_sup_prc.groupby('StoreCodes')))
#print(dfs['WYE'])

# Step5: Subset df by suppliers & Step6: Save dataframes as CSV by stores & suppliers

# Step5-1: Create folders, which will case access denied if pre-created > Included in the loop

#Create folder by store code
#For testing in personal laptop
#Add on 22 March, store files in Personal Laptop for checking
folderpath_list = [r"C:\\\data\Lightyear\06.ScheduledAutomation\02.UploadFiles", r"C:\Users\Reddrop\Desktop\LightYearAutomation\03.UploadFiles"]
# for test folderpath = r"C:\\\data\Lightyear\06.ScheduledAutomation\02.UploadFiles"
# for test folderpath = r"C:\Users\Reddrop\Desktop\LightYearAutomation\03.UploadFiles"

#Execute when the # of files doesn't match the RPA reference table
file_list = []
for path in folderpath_list:
    for strs in stores:
        #Create supplier dict by store
        dfs_str = dict(tuple(dfs[strs].groupby('Supplier')))
        #Create suppliers list of each store, and sort by str
        str_sup = dfs[strs]['Supplier'].unique().tolist()
        str_sup = sorted(str_sup, key=str.lower)
        for sups in str_sup:
            #Create file name by supplier code
            outpath= path + '/' + today + '_' + strs + '_' + sups + '.csv'
            #Execute when the # of files doesn't match the RPA reference table
            file_names =  today + '_' + strs + '_' + sups
            file_list.append(file_names)
            #Subset each store's df by supplier code
            dfs_sup = dict(tuple(dfs_str[sups].groupby('Supplier')))
            #Save each file as csv
            dfs_sup[sups].to_csv(outpath,sep=',',index=False,header=True)
            # Print out result
            print(f'Save out: {outpath}')

#Execute when the # of files doesn't match the RPA reference table
df_file_list = pd.DataFrame(file_list,columns = ['FileName'])

## Step6: Create Vendor Name Lists for RPA
# Step6-1: Import Store Name & Code Reference Table
csv_file4 = r"C:\\\data\Lightyear\06.ScheduledAutomation\01.RawData_ReferenceTable\Ref_StoreNameCode.csv"
df_str_ref = pd.read_csv(csv_file4, low_memory = False, encoding='latin-1')
df_str_ref = df_str_ref.dropna()

# Step6-2: Create Store/supplier reference table
# Modify on 10th March, to make RPA run store by store
# R1: Reference table for RPA loop 1: Select store & reference table of the selected store
# Step6-3: Merge table with supplier

df_str_file_ref = df_str_sup_prc[['StoreCodes']].drop_duplicates().dropna()

df_str_file_ref = pd.merge(df_str_ref
                          ,df_str_file_ref
                          ,on='StoreCodes'
                          , how='inner')

folderpath = r"C:\Users\Reddrop\Desktop\LightYearAutomation\03.UploadFiles"
df_str_file_ref['StoreFilePathName'] = folderpath + '\\' + today + '_' + df_str_file_ref['StoreCodes']  + '.csv'
print(df_str_file_ref.columns[[1]])
df_str_file_ref = df_str_file_ref.drop(df_str_file_ref.columns[[1]], axis=1)  # df.columns is zero-based pd.Index

# Step6-5: Output RPA store selection reference talbe
outpath5= r'C:\\\data\Lightyear\06.ScheduledAutomation\02.UploadFiles' + '\\' + today  + '_0_Store_Selection_File.csv' 
print(outpath5)
df_str_file_ref.to_csv(outpath5,sep=',',index=False,header=True)

# R2: Reference table for RPA loop 2: Reference table of the selected store, same as the original store supplier file name ref
# Step1: Prepare reference table
df_str_sup_ref = df_str_sup_prc[['VENDNAME','StoreCodes','Supplier']].drop_duplicates()

df_str_sup_ref = pd.merge(df_str_ref
                          ,df_str_sup_ref
                          ,on='StoreCodes'
                          , how='inner')

df_str_sup_ref['FileName'] = today + '_' + df_str_sup_ref['StoreCodes'] + '_' + df_str_sup_ref['Supplier']
    
col_sel2 = df_str_sup_ref.columns.values.tolist()
fil2 = col_sel2[0:1] + col_sel2[2:3] + col_sel2[3:4] + col_sel2[-1:]
df_str_sup_ref = df_str_sup_ref[fil2]

# Step2: Save out files by store
dfs_str_ref = dict(tuple(df_str_sup_ref.groupby('StoreCodes')))

store_cd = df_str_sup_ref['StoreCodes'].unique().tolist()
#print(dfs_str_ref['AFW'])

#Execute when the # of files doesn't match the RPA reference table
for path in folderpath_list:
    for strs in store_cd:
        #Create supplier dict by store
        dfs_str_ref = dict(tuple(df_str_sup_ref.groupby('StoreCodes')))
        #Create file name by supplier code
        outpath= path + '/' + today + '_' + strs + '.csv'
        #Save each file as csv
        dfs_str_ref[strs].to_csv(outpath,sep=',',index=False,header=True)

# Step6-5: Output RPA reference talbe
outpath2= r'C:\\\data\Lightyear\06.ScheduledAutomation\02.UploadFiles' + '\\' + today  + '_0_Store_Supplier_File.csv' 
print(outpath2)
df_str_sup_ref.to_csv(outpath2,sep=',',index=False,header=True)

outpath3= r'C:\\\data\Lightyear\06.ScheduledAutomation\02.UploadFiles' + '\\1_All_Lines_' + today + '.csv' 
print(outpath3)
df_str_sup_prc.to_csv(outpath3,sep=',',index=False,header=True)

#Check output files not in RPA reference table
check6 = pd.merge(df_file_list
                  ,df_str_sup_ref
                  ,on='FileName'
                  , how='left')
check6 = check6[check6['VENDNAME'].isna()]

# Step6-6: Create Error Message csv for RPA
outpath4= r'C:\\\data\Lightyear\06.ScheduledAutomation\03.ErrorMessageFiles' + '\\' + today  + '_ErrorMessage_PriceUpdate.csv' 
print(outpath4)
error_msg = pd.DataFrame(columns=['StoreName','VENDNAME','FileName','ErrorMessage'])
error_msg.to_csv(outpath4,sep=',',index=False,header=True)
