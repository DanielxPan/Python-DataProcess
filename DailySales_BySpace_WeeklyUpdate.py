# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 11:13:25 2024

@author: danielp
"""

import pandas as pd
import datetime as dt
import psycopg2
# import shutil

########### Block1: Create common variables ###########


##### Step1: Today Variables
today= dt.date.today()
# For test today= dt.date.today() - dt.timedelta(days=1)
today_str = str(today)
# for test today_str='2023-10-22'
print(today_str)



##### Step2: Date of last week's date range

# Last Sunday, job starts every Monday
last_sunday = today - dt.timedelta(days=1)

# Get str for variable
last_sunday_str = str(last_sunday)

# First day of last quarter
last_monday = today - dt.timedelta(days=7)

# Get str for variable
last_monday_str = str(last_monday)

# Last year last month
last_year_month = today - dt.timedelta(days=396) ### Leave at least 1 month

########### End of Block1: Create common variables ###########



#################### Store level #############################


########### Block2: Connect to HB and create data set ###########

# # Connect to your Database Name database
db_params = {
    'host': "host ip",
    'database': "Database Name",
    'user': "danielpan",
    'password': "password"
}


# Part1-1: Pagination parameters
batch_size = 10000  # You can adjust the batch size as needed

# Check start & end date
print(f'Start Date: {last_monday_str}, End Date{last_sunday_str}')


try:
    ### Step1-1: Find out total row counts of the query
    # Establish a connection to the database
    conn = psycopg2.connect(**db_params)

    # Create a cursor object
    cursor = conn.cursor()


    # Execute the total count query
    total_count_query = f"""
                        SELECT COUNT(*)
                        FROM
                        (
                        --- 24 secs 169,118 rows for 1 month all stores
                        SELECT DISTINCT
                        	ti.shop_title AS Shop
                        	,DATE((ti.transacted_at at time zone 'UTC') at time zone 'Australia/Melbourne') as transacted_date
                            ,COALESCE(SUM(ti.total), 0) AS Daily_sales
                        	,COALESCE(SUM(ti.quantity), 0) AS Daily_QTY
                        	,COALESCE(COUNT (DISTINCT ti.transaction_id), 0) AS Daily_Transaction_Count
                        	
                        FROM transaction_items_partitioned ti --- USing this table will results more 
                        -- FROM public.transaction_items ti
                        LEFT JOIN public.shops s
                        	ON ti.shop_id = s.id
                        WHERE
                        	-- We use the timezone this way cos `transacted_at` is stored in UTC and this doesn't require us to convert the date range causing a full table scan
                        	-- TODO - Change this to the date range you want to look at
                        	ti.transacted_at 	BETWEEN ((('{last_monday_str} 00:00:00'::timestamptz) AT time zone 'UTC') AT TIME ZONE 'Australia/Melbourne')::timestamp 
                        						    AND ((('{last_sunday_str} 23:59:59'::timestamptz) AT time zone 'UTC') AT TIME ZONE 'Australia/Melbourne')::timestamp
                                -- Exclude voided and returned transactions
                        	-- Exclude voided and returned transactions
                        	AND is_returned <> TRUE
                        	AND is_void <> TRUE
                        	AND is_removed <> TRUE
                        --         Filter by Reddrop group
                        	AND shop_id IN 
                        		(
                        		SELECT *
                        		FROM get_shop_ids_in_group(('GroupID')::UUID)
                        		)
                            AND REGEXP_REPLACE(ti.department_title,'^\d+\s*-\s*', '') NOT IN 
                                (
                                'RETURN & EARN CDS',
                                'GIFT CARD'
                                )
                        GROUP BY 1,2
                        -- Sort by total sales
                        ORDER BY 1,2 ASC
                        ) SUB
                        ;
                        """
    cursor.execute(total_count_query)

    # Fetch the total count
    total_count = cursor.fetchone()[0]
    print(f"Total count: {total_count}")

    # Calculate the number of batches
    num_batches = (total_count + batch_size - 1) // batch_size
    print(f"Number of batches: {num_batches}")

    ### End of Step1-1: Find out total row counts of the query

    df_list_store = []
    for batch_num in range(num_batches):
        # Calculate offset and limit for the current batch
        #offset: Represents the starting index of the current batch
        offset = batch_num * batch_size
        # limit: Represents the number of items to retrieve for the current batch
        limit = batch_size
        
        # Execute the main query with pagination
        main_query = f"""
                    --- 24 secs 169,118 rows for 1 month all stores
                    SELECT DISTINCT
                    	ti.shop_title AS Shop
                    	,DATE((ti.transacted_at at time zone 'UTC') at time zone 'Australia/Melbourne') as transacted_date
                        ,COALESCE(SUM(ti.total), 0) AS Daily_sales
                    	,COALESCE(SUM(ti.quantity), 0) AS Daily_QTY
                    	,COALESCE(COUNT (DISTINCT ti.transaction_id), 0) AS Daily_Transaction_Count
                    	
                    FROM transaction_items_partitioned ti --- USing this table will results more 
                    -- FROM public.transaction_items ti
                    LEFT JOIN public.shops s
                    	ON ti.shop_id = s.id
                    WHERE
                    	-- We use the timezone this way cos `transacted_at` is stored in UTC and this doesn't require us to convert the date range causing a full table scan
                    	-- TODO - Change this to the date range you want to look at
                        	ti.transacted_at 	BETWEEN ((('{last_monday_str} 00:00:00'::timestamptz) AT time zone 'UTC') AT TIME ZONE 'Australia/Melbourne')::timestamp 
                        						    AND ((('{last_sunday_str} 23:59:59'::timestamptz) AT time zone 'UTC') AT TIME ZONE 'Australia/Melbourne')::timestamp
                            -- Exclude voided and returned transactions
                    	-- Exclude voided and returned transactions
                    	AND is_returned <> TRUE
                    	AND is_void <> TRUE
                    	AND is_removed <> TRUE
                    --         Filter by Reddrop group
                    	AND shop_id IN 
                    		(
                    		SELECT *
                    		FROM get_shop_ids_in_group(('GroupID')::UUID)
                    		)
                        AND REGEXP_REPLACE(ti.department_title,'^\d+\s*-\s*', '') NOT IN 
                            (
                            'RETURN & EARN CDS',
                            'GIFT CARD'
                            )
                    GROUP BY 1,2
                    -- Sort by total sales
                    ORDER BY 1,2 ASC
                    LIMIT {limit}
                    OFFSET {offset}
                    ;
        """
        cursor.execute(main_query)
        
        rows = cursor.fetchall()
        
        # Create a DataFrame from the fetched rows
        df_store = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
        df_list_store.append(df_store)
        
        print(f"Batch {batch_num + 1} appended to df_list_store")
    
    # Outside of loop but inside of Try
    # Concatenate all DataFrames into a single DataFrame
    df_daily_sales_store = pd.concat(df_list_store, ignore_index=True)

except psycopg2.Error as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()

########### End of Block2: Connect to HB and create data set ###########



########### Block3: Rename df ###########


# df_daily_sales_store.columns


### Part1-2: Change column name
# Create new column names
col_new =   [ 'Store'
            , 'Date'
            , 'Sales'
            , 'QTY'
            , 'Txn_Cnt'
            ]

# Rename columns
dict_col = {old_col: new_col for old_col, new_col in zip(df_daily_sales_store.columns, col_new)}
df_daily_sales_store = df_daily_sales_store.rename(columns=dict_col)

## Step1: Change Store names
# Check output
df_daily_sales_store['Store'].unique().tolist()

########### End of Block3: Rename df ###########



########### Block2-2: Add cafe sales from last week ###########

### Step1: Read files

# Cafe folder
fld_cafe_sales = r"C:\data\DashboardReporting\10.TransactionDataIntegration\07.CafeSales\\"

# Cafe file
file_cafe_sales = fld_cafe_sales + today_str + "_CafeSales.csv"
file_cafe_sales = fld_cafe_sales + today_str + "_CafeSales.xls"
print(file_cafe_sales)

# Read file
df_cafe = pd.read_excel(file_cafe_sales, engine='xlrd')

df_cafe.columns

### Step2: Remove rows
df_cafe = df_cafe[df_cafe['Accounting groups'] == "Total AUD"]


### Step3: Keep columns
df_cafe = df_cafe[['Quantity', 'Amount taxed']]
df_cafe = df_cafe.dropna()


### Step4: Add new columns

### Add Date
df_cafe['Date'] = last_monday_str

### Convert Date into date format
df_cafe['Date'] = pd.to_datetime(df_cafe['Date'],
                                 format="%Y-%m-%d").dt.date

### Rename columns as QTY & Sales
df_cafe = df_cafe.rename(columns={'Quantity':'QTY','Amount taxed':'Sales'})


### Step5: Convert QTY & Sales into float
df_cafe.dtypes
# df_cafe['Sales'] = df_cafe['Sales'].replace({",":""},regex=True).astype(float)
df_cafe['QTY'] = df_cafe['QTY'].replace({",":""},regex=True).astype(float)


### Add columns Store	Department	Space Txn_Cnt	Store Code

df_cafe['Store'] = 'Wye River Cafe Store'
df_cafe['Txn_Cnt'] = df_cafe['QTY']


### Step6: Append cafe sales
df_daily_sales_store = df_daily_sales_store.append(df_cafe)

########### End of Block2-2: Add cafe sales from last week ###########



##### Step3: Add store code

### Read store ref table
# file_store_ref = r"C:\data\DashboardReporting\08.Tanda_CostsByShiftTeam\00.Reference\StoreCode.csv"
file_store_ref = r"C:\data\DashboardReporting\10.TransactionDataIntegration\08.Reference\Referece_SalesReport_StoreLevel.csv"
df_ref_store = pd.read_csv(file_store_ref)
df_daily_sales_store.dtypes


### Join store code
df_daily_sales_store = pd.merge(df_daily_sales_store
                          ,df_ref_store
                          ,how='left'
                          ,on='Store')

########### End of Block3: Add store abbreviation ###########



########### Block3: Append the latest daily sales ###########


### Step1: Read the latest sales cube
### Use file names to find last update date and file
path_sales_data = r"C:\data\DashboardReporting\10.TransactionDataIntegration\04.ProcessedFile\\"
# Create empty list
list_file = []

import os

# Add all filename into list
for filename in os.listdir(path_sales_data):
    if '_DailySales_Store.csv' in filename:
        list_file.append(filename)
    else:
        print(f'{filename} is not a weekly sales file.')

# Conver list into df
df_file = pd.DataFrame(list_file, columns=['FileName'])

# Extract date from filename
df_file['FileDate'] = df_file['FileName'].str[:10]

# Conver file date into datetime
df_file['FileDate'] = pd.to_datetime(df_file['FileDate'], format = '%Y-%m-%d')

# Sort df
df_file = df_file.sort_values(by=['FileDate'], ascending=True)

# Find the latest 2 date
df_last_update = df_file.iloc[-1:,:]

# Get a list of file name
old_file = df_last_update.iloc[0,0]
print(f"Latest file: {old_file}")

# File Full Path
path_old_file = path_sales_data + old_file
print(path_old_file)

df_old_file = pd.read_csv(path_old_file, low_memory = False)

### Step2: Append new week's sales

df_daily_sales_store_appened = pd.concat([df_daily_sales_store,
                                           df_old_file],
                                           ignore_index=True)

### Step3: Delete outdated sales (Leave at least a month in case different week number in different years)

# Change to date time
df_daily_sales_store_appened['Date'] = pd.to_datetime(df_daily_sales_store_appened['Date']).dt.date


########## Noted on 27 Nov, try to keep Store level sales from the start of 2023 to do YOY comparison
# Filter date less then last month
# fil_last_month = df_daily_sales_store_appened['Date'] >= last_year_month

# Apply filter 
# df_daily_sales_store_appened = df_daily_sales_store_appened[fil_last_month]

########### End of Block3: Append the latest daily sales ###########





########### Block4: Create hierarchy reference table ###########

########### Block5: Save out result ###########

### Set up date format
dt_format = "%Y/%m/%d"

######### R-drive

##### Step1: Create folder variables / Path

### Folder
folder_out_R = r"C:\data\DashboardReporting\10.TransactionDataIntegration\04.ProcessedFile\\"

### Path
path_sales_file_R = folder_out_R + today_str + "_DailySales_Store.csv"
print(path_sales_file_R)

##### Step2: Save out as csv


df_daily_sales_store_appened.to_csv(path_sales_file_R
                                  ,sep=','
                                  ,index=False
                                  ,header=True
                                  , date_format=dt_format)



######### Local

##### Step1: Create folder variables / Path

### Folder
folder_out_local = r"C:\Users\Reddrop\Desktop\SalesReport\\"

### Path
path_sales_file_local = folder_out_local + "DailySales_Store.csv"
print(path_sales_file_local)

##### Step2: Save out as csv


df_daily_sales_store_appened.to_csv(path_sales_file_local
                                  ,sep=','
                                  ,index=False
                                  ,header=True
                                  , date_format=dt_format)

#################### End of Store level #############################

#

#

#

#################### Department level #############################

########### Block2: Connect to HB and create data set ###########

# # Connect to your Database Name database
db_params = {
    'host': "host ip",
    'database': "Database Name",
    'user': "danielpan",
    'password': "password"
}


# Part1-1: Pagination parameters
batch_size = 10000  # You can adjust the batch size as needed

# Check start & end date
print(f'Start Date: {last_monday_str}, End Date{last_sunday_str}')


try:
    ### Step1-1: Find out total row counts of the query
    # Establish a connection to the database
    conn = psycopg2.connect(**db_params)

    # Create a cursor object
    cursor = conn.cursor()


    # Execute the total count query
    total_count_query = f"""
                        SELECT COUNT(*)
                        FROM
                        (
                        --- 24 secs 169,118 rows for 1 month all stores
                        SELECT DISTINCT
                        	ti.shop_title AS Shop
                        	,REGEXP_REPLACE(ti.department_title,'^\d+\s*-\s*', '') AS Department
                        	,REGEXP_REPLACE(ti.space_title,'^\d+\s*-\s*', '') AS Space
                        	---,REGEXP_REPLACE(ti.category_title,'^\d+\s*-\s*', '') AS Category
                        	,DATE((ti.transacted_at at time zone 'UTC') at time zone 'Australia/Melbourne') as transacted_date
                            ,COALESCE(SUM(ti.total), 0) AS Daily_sales
                        	,COALESCE(SUM(ti.quantity), 0) AS Daily_QTY
                        	,COALESCE(COUNT (DISTINCT ti.transaction_id), 0) AS Daily_Transaction_Count
                        	
                        FROM transaction_items_partitioned ti --- USing this table will results more 
                        -- FROM public.transaction_items ti
                        LEFT JOIN public.shops s
                        	ON ti.shop_id = s.id
                        WHERE
                        	-- We use the timezone this way cos `transacted_at` is stored in UTC and this doesn't require us to convert the date range causing a full table scan
                        	-- TODO - Change this to the date range you want to look at
                        	ti.transacted_at 	BETWEEN ((('{last_monday_str} 00:00:00'::timestamptz) AT time zone 'UTC') AT TIME ZONE 'Australia/Melbourne')::timestamp 
                        						    AND ((('{last_sunday_str} 23:59:59'::timestamptz) AT time zone 'UTC') AT TIME ZONE 'Australia/Melbourne')::timestamp
                                -- Exclude voided and returned transactions
                        	-- Exclude voided and returned transactions
                        	AND is_returned <> TRUE
                        	AND is_void <> TRUE
                        	AND is_removed <> TRUE
                        --         Filter by Reddrop group
                        	AND shop_id IN 
                        		(
                        		SELECT *
                        		FROM get_shop_ids_in_group(('GroupID')::UUID)
                        		)
                            AND REGEXP_REPLACE(ti.department_title,'^\d+\s*-\s*', '') NOT IN 
                                (
                                'RETURN & EARN CDS',
                                'GIFT CARD'
                                )
                        GROUP BY 1,2,3,4
                        -- Sort by total sales
                        ORDER BY 1,4,2,3 ASC
                        ) SUB
                        ;
                        """
    cursor.execute(total_count_query)

    # Fetch the total count
    total_count = cursor.fetchone()[0]
    print(f"Total count: {total_count}")

    # Calculate the number of batches
    num_batches = (total_count + batch_size - 1) // batch_size
    print(f"Number of batches: {num_batches}")

    ### End of Step1-1: Find out total row counts of the query

    df_list = []
    for batch_num in range(num_batches):
        # Calculate offset and limit for the current batch
        #offset: Represents the starting index of the current batch
        offset = batch_num * batch_size
        # limit: Represents the number of items to retrieve for the current batch
        limit = batch_size
        
        # Execute the main query with pagination
        main_query = f"""
                    --- 24 secs 169,118 rows for 1 month all stores
                    SELECT DISTINCT
                    	ti.shop_title AS Shop
                    	,REGEXP_REPLACE(ti.department_title,'^\d+\s*-\s*', '') AS Department
                    	,REGEXP_REPLACE(ti.space_title,'^\d+\s*-\s*', '') AS Space
                    	---,REGEXP_REPLACE(ti.category_title,'^\d+\s*-\s*', '') AS Category
                    	,DATE((ti.transacted_at at time zone 'UTC') at time zone 'Australia/Melbourne') as transacted_date
                        ,COALESCE(SUM(ti.total), 0) AS Daily_sales
                    	,COALESCE(SUM(ti.quantity), 0) AS Daily_QTY
                    	,COALESCE(COUNT (DISTINCT ti.transaction_id), 0) AS Daily_Transaction_Count
                    	
                    FROM transaction_items_partitioned ti --- USing this table will results more 
                    -- FROM public.transaction_items ti
                    LEFT JOIN public.shops s
                    	ON ti.shop_id = s.id
                    WHERE
                    	-- We use the timezone this way cos `transacted_at` is stored in UTC and this doesn't require us to convert the date range causing a full table scan
                    	-- TODO - Change this to the date range you want to look at
                        	ti.transacted_at 	BETWEEN ((('{last_monday_str} 00:00:00'::timestamptz) AT time zone 'UTC') AT TIME ZONE 'Australia/Melbourne')::timestamp 
                        						    AND ((('{last_sunday_str} 23:59:59'::timestamptz) AT time zone 'UTC') AT TIME ZONE 'Australia/Melbourne')::timestamp
                            -- Exclude voided and returned transactions
                    	-- Exclude voided and returned transactions
                    	AND is_returned <> TRUE
                    	AND is_void <> TRUE
                    	AND is_removed <> TRUE
                    --         Filter by Reddrop group
                    	AND shop_id IN 
                    		(
                    		SELECT *
                    		FROM get_shop_ids_in_group(('GroupID')::UUID)
                    		)
                        AND REGEXP_REPLACE(ti.department_title,'^\d+\s*-\s*', '') NOT IN 
                            (
                            'RETURN & EARN CDS',
                            'GIFT CARD'
                            )
                    GROUP BY 1,2,3,4
                    -- Sort by total sales
                    ORDER BY 1,4,2,3 ASC
                    LIMIT {limit}
                    OFFSET {offset}
                    ;
        """
        cursor.execute(main_query)
        
        rows = cursor.fetchall()
        
        # Create a DataFrame from the fetched rows
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
        df_list.append(df)
        
        print(f"Batch {batch_num + 1} appended to df_list")
    
    # Outside of loop but inside of Try
    # Concatenate all DataFrames into a single DataFrame
    df_daily_sales = pd.concat(df_list, ignore_index=True)

except psycopg2.Error as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()


########### End of Block2: Connect to HB and create data set ###########



########### Block3: Rename df ###########


df_daily_sales.columns


### Part1-2: Change column name
# Create new column names
col_new =   [ 'Store'
            , 'Department'
            , 'Space'
            , 'Date'
            , 'Sales'
            , 'QTY'
            , 'Txn_Cnt'
            ]

# Rename columns
dict_col = {old_col: new_col for old_col, new_col in zip(df_daily_sales.columns, col_new)}
df_daily_sales = df_daily_sales.rename(columns=dict_col)

## Step1: Change Store names

# Check output
df_daily_sales['Store'].unique().tolist()


########### End of Block3: Rename df & append the latest df ###########



########### Block2-2: Add cafe sales from last week ###########

### Add department & space
df_cafe['Department'] = 'Cafe Store'
df_cafe['Space'] = 'Cafe Store'

### Step5: Append cafe sales
df_daily_sales = df_daily_sales.append(df_cafe)

########### End of Block2-2: Add cafe sales from last week ###########



##### Step3: Add store code

### Read store ref table
# file_store_ref = r"C:\data\DashboardReporting\08.Tanda_CostsByShiftTeam\00.Reference\StoreCode.csv"
file_store_ref = r"C:\data\DashboardReporting\10.TransactionDataIntegration\08.Reference\Referece_SalesReport_StoreLevel.csv"
df_ref_store = pd.read_csv(file_store_ref)
df_ref_store.dtypes


### Join store code
df_daily_sales = pd.merge(df_daily_sales
                          ,df_ref_store
                          ,how='left'
                          ,on='Store')


### Add an unique key
df_daily_sales['Store_Hier_Key'] = df_daily_sales[['Store Code'
                                                   ,'Department'
                                                   ,'Space' ]].astype(str).agg('_'.join,
                                                                             axis=1)


########### End of Block3: Add store abbreviation ###########



########### Block3: Append the latest daily sales ###########


### Step1: Read the latest sales cube
### Use file names to find last update date and file
path_sales_data = r"C:\data\DashboardReporting\10.TransactionDataIntegration\04.ProcessedFile\\"
# Create empty list
list_file = []

import os

# Add all filename into list
for filename in os.listdir(path_sales_data):
    if '_DailySales_Department.csv' in filename:
        list_file.append(filename)
    else:
        print(f'{filename} is not a weekly sales file.')

# Conver list into df
df_file = pd.DataFrame(list_file, columns=['FileName'])

# Extract date from filename
df_file['FileDate'] = df_file['FileName'].str[:10]

# Conver file date into datetime
df_file['FileDate'] = pd.to_datetime(df_file['FileDate'], format = '%Y-%m-%d')

# Sort df
df_file = df_file.sort_values(by=['FileDate'], ascending=True)

# Find the latest 2 date
df_last_update = df_file.iloc[-1:,:]

# Get a list of file name
old_file = df_last_update.iloc[0,0]
print(f"Latest file: {old_file}")

# File Full Path
path_old_file = path_sales_data + old_file
print(path_old_file)

df_old_file = pd.read_csv(path_old_file, low_memory = False)

### Step2: Append new week's sales

df_daily_sales_appened = pd.concat([df_daily_sales,
                                   df_old_file],
                                   ignore_index=True)


### Step3: Delete outdated sales (Leave at least a month in case different week number in different years)

# Change to date time
df_daily_sales_appened['Date'] = pd.to_datetime(df_daily_sales_appened['Date']).dt.date


##### Noted on 2nd of DEC to keep sales from the start of 2023
# # Filter date less then last month
# fil_last_month = df_daily_sales_appened['Date'] >= last_year_month

# # Apply filter 
# df_daily_sales_appened = df_daily_sales_appened[fil_last_month]


### Check min and max date by stores
df_check = df_daily_sales_appened.groupby(['Store'])['Date'].agg(['min','max']).reset_index()

########### End of Block3: Append the latest daily sales ###########



########### Block4: Create hierarchy reference table ###########

########### Block5: Save out result ###########

### Set up date format
dt_format = "%Y/%m/%d"


######### Space level is too big to save in 2 environments TBD

##### Step1: Create folder variables / Path

### Folder
folder_out_R = r"C:\data\DashboardReporting\10.TransactionDataIntegration\04.ProcessedFile\\"

### Path
path_sales_file_R = folder_out_R + today_str + "_DailySales_Department.csv"
print(path_sales_file_R)

##### Step2: Save out as csv


df_daily_sales_appened.to_csv(path_sales_file_R
                              ,sep=','
                              ,index=False
                              ,header=True
                              , date_format=dt_format)


######### Local

##### Step1: Create folder variables / Path

### Folder
folder_out_local = r"C:\Users\Reddrop\Desktop\SalesReport\\"

### Path
path_sales_file_local = folder_out_local + "DailySales_Department.csv"
print(path_sales_file_local)

##### Step2: Save out as csv


df_daily_sales_appened.to_csv(path_sales_file_local
                              ,sep=','
                              ,index=False
                              ,header=True
                              , date_format=dt_format)


#################### End of Department level #############################
