# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 10:11:09 2024

@author: danielp
"""


# importing all the required modules
import PyPDF2
import pandas as pd
import datetime as dt
import psycopg2
import re
import os
import pdfplumber

########### Block1: Create common variables ###########

##### Step1: Today Variables

today= dt.date.today()
# For test today= dt.date.today() - dt.timedelta(days=7)  
today_str = str(today)
# for test today_str='2023-10-22'
print(today_str)

########### End of Block1: Create common variables ###########



################# For Supplier1

########## Block2-1: Read PDFs & Convert into DFs ##########

### Step0: Need to restore weekly cafe sales since last year
fld_statements = r"C:\data\Lightyear\10.Download_Statement_Automation\03.Files\\"



### Empty list to append match lines
list_dfs_Supplier1 = []

### File key words
file_keyword = today_str + '_Supplier1'

### Start/ stop Key words
start_words = ["Balance Overdue Due by: Due by: Due by: Due on/after:",
               "Balance Overdue Due by: Due by: Due by:",
               "Balance Overdue"]
stop_words = "Date Reference Description Due Debit Credit Balance"

### For loop
for filenames in os.listdir(fld_statements):
    
    ### Get file from today
    if file_keyword in filenames and ".pdf" in filenames:

        ### Extract Store, Suppliers
        supplier = filenames[11:14]
        store = filenames[15:-4]
        print(f"File for {store} from {supplier}")
        ### Get full path
        file_full_path = os.path.join(fld_statements,filenames)
        # Read PDF and get qty & sales
        with pdfplumber.open(file_full_path) as pdf:
            # Extract tables from the first page
            first_page = pdf.pages[0]
            
            # Attempt to extract tables directly
            table = first_page.extract_tables()
            
            # Attempt to extract raw text directly
            text = first_page.extract_text()
            
            ### Mke text into lines
            lines = text.split('\n')
            
            ### Find index of start & stop words
            start_index = next((i for i, line in enumerate(lines) if any(word in line for word in start_words)),None)
            end_index = next((i for i, line in enumerate(lines) if stop_words in line),None)
            
            ### Apply filter
            lines_filtered = lines[start_index:end_index]
            
            ### Select splict each line as values of dfs
            # (?<!:) The negative lookbehind ensures the splitting respects the format of elements that include a colon.
            headers = re.findall(r'Due by:|Due on/after:|[^\s]+',lines_filtered[0])
            print(headers)
            dates = lines_filtered[1].split()    # Second line as the 'dates' row. split() function by space
            dates.insert(1,dates[0])
            values = re.split(r'\s+', lines_filtered[2])  # Third line as the 'values' row (split by spaces or multiple spaces)
            print(dates)
            ### Create dict
            data = {"Item": headers,
                    "Date": dates,
                    "Amt": values}
            
            ### Convert dict into dfs
            df_Supplier1 = pd.DataFrame(data)
                
            ### Add other attributes
            df_Supplier1['Store'] = store
            df_Supplier1['Supplier'] = supplier
            
            ### Append df into the list
            list_dfs_Supplier1.append(df_Supplier1)

### Concate as 1 df
df_Supplier1 = pd.concat(list_dfs_Supplier1)



########## Block2-2: Process df ##########

### Step1: Convert data string to date format

df_Supplier1['Date'] = pd.to_datetime(df_Supplier1['Date'], format = '%d/%m/%Y').dt.date

### Step2: Convert strings to float

df_Supplier1['Amt'] = df_Supplier1['Amt'].str.replace(',','').astype(float)

### Step3: Replace all due date items as DueDate

df_Supplier1.loc[df_Supplier1['Item'] == 'Due on/after:', 'Item'] = 'Due by:'



########## Block2-3: Pivot table ##########


### Step1: Pivot table
df_Supplier1.columns
df_Supplier1_pvt = df_Supplier1.pivot_table(index=['Item', 'Date', 'Supplier'],
                                values='Amt',
                                columns='Store').reset_index()

### Step2: Sort by date
df_Supplier1_pvt.sort_values(by='Date',inplace=True)

### Step3: Fill na as 0

df_Supplier1_pvt = df_Supplier1_pvt.fillna(0)

### Step4: Sum all stores
df_Supplier1_pvt.columns
list_stores = df_Supplier1_pvt.columns.tolist()
list_stores = list_stores[3:]
df_Supplier1_pvt['Total'] = df_Supplier1_pvt[list_stores].sum(axis=1)


########## Block2-4: Add bank details ##########

### Check shape of the df
df_Supplier1_pvt.shape[1]

### Add a list of bank information

list_bank = ['Bank','','Supplier1']
list_bank = list_bank + ['BankName'] * (df_Supplier1_pvt.shape[1]-4)
list_bank = list_bank + ['']

### Convert list into df
df_Supplier1_bank_details = pd.DataFrame([list_bank],columns=df_Supplier1_pvt.columns.tolist())

### Append with the original df
df_Supplier1_pvt_bank = pd.concat([df_Supplier1_bank_details,df_Supplier1_pvt],
                            ignore_index=True)

########## Block2-4: Save out result files ##########

# ### Step1: Create folder/file variables for AP process files
# fld_ap = r"C:\data\DashboardReporting\11.ZAP_BI\01.OutstandingAP_Supplier1_Supplier2\04.ProcessedFiles\\"

# file_ap_Supplier1 = fld_ap + today_str + "_Supplier1_AP_Forcast.csv"
# print(file_ap_Supplier1)

# ### Step2: Save out csv
# df_Supplier1_pvt.to_csv(file_ap_Supplier1, index=False, header=True)

################# End of process for Supplier1



################# For Metcash

########## Block2-1: Read PDFs & Convert into DFs ##########


### Empty list to append match lines
list_dfs_Supplier2 = []

### File key words
file_keyword = today_str + '_Supplier2'

### Start/ stop Key words
start_words = ["Balance Overdue Due by: Due by: Due by: Due on/after:",
               "Balance Overdue Due by: Due by: Due by:",
               "Balance Overdue"]
stop_words = "Date Reference Description Due Debit Credit Balance"

### For loop
for filenames in os.listdir(fld_statements):
    
    ### Get file from today
    if file_keyword in filenames and ".pdf" in filenames:
            
            ### Process Metcash
            ### Extract Store, Suppliers
            supplier = filenames[11:18]
            store = filenames[19:-4]
            print(f"File for {store} from {supplier}")
            ### Get full path
            file_full_path = os.path.join(fld_statements,filenames)
        
            # Read PDF and get qty & sales
            with pdfplumber.open(file_full_path) as pdf:
                # Extract tables from the first page
                first_page = pdf.pages[0]
                
                # Attempt to extract tables directly
                table = first_page.extract_tables()
                
                # Attempt to extract raw text directly
                text = first_page.extract_text()
                
                ### Mke text into lines
                lines = text.split('\n')
                
                ### Find index of start & stop words
                start_index = next((i for i, line in enumerate(lines) if any(word in line for word in start_words)),None)
                end_index = next((i for i, line in enumerate(lines) if stop_words in line),None)
                
                ### Apply filter
                lines_filtered = lines[start_index:end_index]
                
                ### Select splict each line as values of dfs
                # (?<!:) The negative lookbehind ensures the splitting respects the format of elements that include a colon.
                headers = re.findall(r'Due by:|Due on/after:|[^\s]+',lines_filtered[0])
                print(headers)
                dates = lines_filtered[1].split()    # Second line as the 'dates' row. split() function by space
                dates.insert(1,dates[0])
                values = re.split(r'\s+', lines_filtered[2])  # Third line as the 'values' row (split by spaces or multiple spaces)
                print(dates)
                ### Create dict
                data = {"Item": headers,
                        "Date": dates,
                        "Amt": values}
                
                ### Convert dict into dfs
                df_Supplier2 = pd.DataFrame(data)
                    
                ### Add other attributes
                df_Supplier2['Store'] = store
                df_Supplier2['Supplier'] = supplier
                
                ### Append df into the list
                list_dfs_Supplier2.append(df_Supplier2)

### Concate as 1 df
df_Supplier2 = pd.concat(list_dfs_Supplier2)



########## Block2-2: Process df ##########

### Step1: Convert data string to date format

df_Supplier2['Date'] = pd.to_datetime(df_Supplier2['Date'], format = '%d/%m/%Y').dt.date

### Step2: Convert strings to float

df_Supplier2['Amt'] = df_Supplier2['Amt'].str.replace(',','').astype(float)

### Step3: Replace all due date items as DueDate

df_Supplier2.loc[df_Supplier2['Item'] == 'Due on/after:', 'Item'] = 'Due by:'



########## Block2-3: Pivot table ##########

### Step1: Pivot table
df_Supplier2.columns
df_Supplier2_pvt = df_Supplier2.pivot_table(index=['Item', 'Date', 'Supplier'],
                                values='Amt',
                                columns='Store').reset_index()

### Step2: Sort by date
df_Supplier2_pvt.sort_values(by='Date',inplace=True)

### Step3: Fill na as 0

df_Supplier2_pvt = df_Supplier2_pvt.fillna(0)

### Step4: Sum all stores
df_Supplier2_pvt.columns
list_stores = df_Supplier2_pvt.columns.tolist()
list_stores = list_stores[3:]
df_Supplier2_pvt['Total'] = df_Supplier2_pvt[list_stores].sum(axis=1)



########## Block2-4: Add bank details ##########

### Check shape of the df
df_Supplier2_pvt.shape[1]

### Add a list of bank information

list_bank = ['Bank','','Metcash']
list_bank = list_bank + ['BankName'] * (df_Supplier2_pvt.shape[1]-4)
list_bank = list_bank + ['']

### Convert list into df
df_Supplier2_bank_details = pd.DataFrame([list_bank],columns=df_Supplier2_pvt.columns.tolist())

### Append with the original df
df_Supplier2_pvt_bank = pd.concat([df_Supplier2_bank_details,df_Supplier2_pvt],
                            ignore_index=True)


########## Block3: Save out result for suppliers ##########

### Step1: Create folder/file variables for AP process files
fld_ap = r"C:\data\DashboardReporting\11.ZAP_BI\01.OutstandingAP_Supplier1_Supplier2\04.ProcessedFiles\\"

file_ap_forcast = fld_ap + today_str + "_AP_Forecast.xlsx"
print(file_ap_forcast)

### Step2: Save out as excel

# Save DataFrames to Excel with formatting
with pd.ExcelWriter(file_ap_forcast, engine='xlsxwriter') as writer:
    # Write DataFrames to specific sheets
    df_Supplier1_pvt_bank.to_excel(writer, sheet_name='Supplier1', index=False, header=True)
    df_IGA_pvt_bank.to_excel(writer, sheet_name='Supplier2', index=False, header=True)

    # Access the workbook and sheets
    workbook = writer.book
    sheet_Supplier1 = writer.sheets['Supplier1']
    sheet_Supplier2 = writer.sheets['Supplier2']

    # Define formats
    integer_format = workbook.add_format({'num_format': '#,##0'})  # Integer with commas
    border_format = workbook.add_format({'border': 1})  # Gridline format with borders

    # Apply formatting to each sheet
    for sheet_name, df in [('Supplier1', df_Supplier1_pvt_bank), ('Supplier2', df_Supplier2_pvt_bank)]:
        sheet = writer.sheets[sheet_name]

        # Get dimensions of the dataframe
        rows, cols = df.shape

        # Set column widths and formats
        for col_num, value in enumerate(df.columns):
            sheet.set_column(col_num, col_num, len(str(value)) + 2)  # Auto adjust column width

        # Add gridlines (apply border format to the entire table)
        sheet.conditional_format(0, 0, rows, cols - 1, {'type': 'no_errors', 'format': border_format})

        # Apply integer formatting to numerical columns
        num_columns = df.select_dtypes(include=['int', 'float']).columns
        for col_num, column_name in enumerate(df.columns):
            if column_name in num_columns:
                sheet.set_column(col_num, col_num, None, integer_format)

