
import pandas as pd
import datetime as dt

# Common used variables
fmt_csv = '.csv'
fmt_xlsx = '.xlsx'
path_raw_data = r"C:\Data\Administration\RMG\HR\ Academy\02.RawData\\"
path_ref_data = r"C:\Data\Administration\RMG\HR\ Academy\04.ReferenceTable\\"

# Import Update Products Lists
today = dt.date.today()
today_str=str(today)

# For test. Remember to note it after test
# for test today_str='2024-06-19'
# today = today - dt.timedelta(days=1)
# today_str=str(today)

print(today_str)

###### Step1: Read Tanda CSV Files
### Use file names to find last update date and file
path_raw_data = r"C:\Data\Administration\RMG\HR\ Academy\02.RawData\\"
# Create empty list
list_file = []

import os

# Add all filename into list
for filename in os.listdir(path_raw_data):
    if '_Staff_Details.csv' in filename:
        list_file.append(filename)
    else:
        print(f'{filename} is not Tanda file')

# Conver list into df
df_file = pd.DataFrame(list_file, columns=['FileName'])

# Extract date from filename
df_file['FileDate'] = df_file['FileName'].str[:10]

# Conver file date into datetime
df_file['FileDate'] = pd.to_datetime(df_file['FileDate'], format = '%Y-%m-%d')

# Sort df
df_file = df_file.sort_values(by=['FileDate'], ascending=True)

# Find the latest 2 date
df_last_update = df_file.iloc[-2:,:]

# Get a list of file name
old_file = df_last_update.iloc[0,0]
new_file = df_last_update.iloc[1,0]

# File Full Path
path_old_file = path_raw_data + old_file
print(path_old_file)

df_old_file = pd.read_csv(path_old_file, low_memory = False)

# File Full Path
path_new_file = path_raw_data + new_file
print(path_new_file)

df_new_file = pd.read_csv(path_new_file, low_memory = False)

###### End of Step1: Read Tanda CSV Files



###### Step2: Read RA Full user list


###### Use file names to find last update date and file from RA full list
# path_raw_data = r"C:\Data\Administration\RMG\HR\ Academy\02.RawData\99.HistoricalDataset_BeforeRPA\\"
path_ra_data = r"C:\Data\Administration\RMG\HR\ Academy\06.FullUserListFromRA\\"
# Create empty list
list_ra_file = []

import os

# Add all filename into list
for filename in os.listdir(path_ra_data):
    if 'Academy_FullUserList.csv' in filename:
        list_ra_file.append(filename)
        print(filename)
    else:
        print(f'{filename} is not RA file')

# Conver list into df
df_ra_file = pd.DataFrame(list_ra_file, columns=['FileName'])

# Extract date from FileName
df_ra_file['FileDate'] = df_ra_file['FileName'].str[:10]

# Convert date str into the date
df_ra_file['FileDate'] = pd.to_datetime(df_ra_file['FileDate'], format = '%Y-%m-%d')

# Select the max date
date_last_ra_file = df_ra_file['FileDate'].max()
print(date_last_ra_file)

# Filter the file name from last RA date
fil_last_update = df_ra_file['FileDate'] == date_last_ra_file
df_ra_file_last_update = df_ra_file[fil_last_update]

filename_last_ra_update = df_ra_file_last_update['FileName'].iloc[0]

# Create file name
file_last_ra_download = path_ra_data + filename_last_ra_update
print(file_last_ra_download)

# Import last upload data
df_last_ra_download = pd.read_csv(file_last_ra_download, low_memory = False)#防止弹出警告



### Process RA
### Criteria 1 - Match by name
# Create trim and upper name
df_last_ra_download.dtypes
df_last_ra_download['first_name'] = df_last_ra_download['first_name'].str.replace('  ',' ')
df_last_ra_download['first_name'] = df_last_ra_download['first_name'].str.strip().str.upper()

df_last_ra_download['last_name'] = df_last_ra_download['last_name'].str.replace('  ',' ')
df_last_ra_download['last_name'] = df_last_ra_download['last_name'].str.strip().str.upper()

# Create full name
df_last_ra_download['Full Name'] = df_last_ra_download['first_name'] + ' ' + df_last_ra_download['last_name']


###### End of Use file names to find last update date and file from RA full list


### Compare names in new file but not in old file
# Create full name list from RA
list_last_ra_update_name =df_last_ra_download['Full Name'].unique().tolist()

# Create full name list from Tanda
list_old_name = df_old_file['NAME'].values.tolist()

# Combine full name list
list_full_name_all = list(set(list_last_ra_update_name + list_old_name))

# Create filter
fil_name = df_new_file['NAME'].isin(list_full_name_all)

# Apply filter
df_raw = df_new_file[~fil_name]



### Compare with username as well to avoid duplicates. Added on 10th July.
# Create list of Passcode from tanda
list_old_username = df_old_file['PASSCODE'].unique().astype(str).tolist()
# pad 0
df_old_file['PASSCODE'] = df_old_file['PASSCODE'].astype(str)
list_old_username_pad_0 = df_old_file['PASSCODE'].str.zfill(5).unique().tolist()
list_old_username_tanda = list(set(list_old_username + list_old_username_pad_0))

# Create list of passcode from RA
list_username_ra = df_last_ra_download['username'].unique().astype(str).tolist()
# pad 0
list_username_ra_pad_0 = df_last_ra_download['username'].astype(str).str.zfill(5).unique().tolist()
list_username_ra_all = list(set(list_username_ra + list_username_ra_pad_0))

# Combine all lists together
list_username_tanda_ra = list(set(list_old_username_tanda + list_username_ra_all))

# Ensure all passcodes in df_raw are strings
df_raw['PASSCODE'] = df_raw['PASSCODE'].astype(str)

# Filter from last upload list by full name
fil_username = df_raw['PASSCODE'].isin(list_username_tanda_ra)

# Apply filter
df_raw = df_raw[~fil_username]

### Add on 15th AUG compare with email address
# Email from Tanda
# lower email list
df_old_file['EMAIL'] = df_old_file['EMAIL'].str.strip().str.lower()
# Create list
list_old_email_tanda = df_old_file['EMAIL'].unique().tolist()

# Email from RA
# lower email list
df_last_ra_download['email'] = df_last_ra_download['email'].str.strip().str.lower()
# Create list
list_old_email_ra = df_last_ra_download['email'].unique().tolist()

# Combine list
list_email_tanda_ra = list(set(list_old_email_tanda + list_old_email_ra))

# Create filter for email
fil_email = df_raw['EMAIL'].isin(list_email_tanda_ra)

# Apply filter
df_raw = df_raw[~fil_email]



##### End of using file name to find the latest date



###### Step2: Filter rename and organize columns

### Step2-1: Filter col list to filter
fil_col = [ 'NAME'
           # ,'EMPLOYEE NUMBER'
           ,'PASSCODE'
           ,'EMAIL'
           ,'DEFAULT LOCATION NAME'
           ,'DATE OF BIRTH'
           ,'EMPLOYMENT START DATE'
          ]

df_raw_filtered = df_raw[fil_col]

### Rename columns
# Create new name list
list_new_col_name = [
                    'Name'
                    ,'Username'
                    ,'Primary Email'
                    ,'Location'
                    ,'Date of Birth'
                    ,'Date Hired'
                    ]

# Create dict
dict_column_names = {old: new for old, new in zip(fil_col,list_new_col_name)}

# Apply dict
df_raw_filtered = df_raw_filtered.rename(columns=dict_column_names)



### Step3-1: Import reference data
file_ref = path_ref_data + 'StoreManagerUserID' + fmt_xlsx
print(file_ref)

df_ref = pd.read_excel(file_ref)

# Step2: Create new columns
df_raw_ref = pd.merge(left=df_raw_filtered
                      ,right=df_ref
                      ,on=['Location']
                      ,how='inner')



###### Step 4: Create default columns & Spilt names to First and Last name
# Create pre-filled columns
df_raw_ref['Password'] = '@1'
df_raw_ref['Language'] = 'english_uk'
df_raw_ref['Training Academy Department'] = 'Customer Service'
df_raw_ref['Level'] = 'user'
df_raw_ref['Store Address'] = ''
df_raw_ref['State'] = ''
df_raw_ref['Zipcode'] = ''
df_raw_ref['Phone'] = ''
df_raw_ref['Timezone'] = 'Australia/Melbourne'
df_raw_ref['Country'] = 'Australia'
df_raw_ref['Training Academy Department'] = 'Customer Service'
df_raw_ref['Default Jobs (HR)'] = 'Retail Assistant'
df_raw_ref['Branch Code'] = df_raw_ref['Branch Name']
df_raw_ref['Store Name'] = df_raw_ref['Branch Name']
df_raw_ref['Store Code'] = df_raw_ref['Branch Name']
# df_raw_ref['City'] = df_raw_ref['Branch Name']

### Spilt names
#The expand=True parameter creates a DataFrame from the split operation
split_names = df_raw_ref['Name'].str.split(' ', expand = True)
# Use column name to filter column
df_raw_ref['First Name'] = split_names[0]
# Use iloc[] to select all columns and rows. Then, apply function / axis = 1 means all rows
df_raw_ref['Last Name'] = split_names.iloc[:,1:].apply(lambda x: ' '.join(x.dropna()), axis=1)

###### Step 5: Format date columns 

### Date Hired
# If you need to display the date in a specific format later, you can use dt.strftime when needed
df_raw_ref['Date Hired'] = pd.to_datetime(df_raw_ref['Date Hired'], format = '%Y-%m-%d')
df_raw_ref['Date Hired'] = df_raw_ref['Date Hired'].dt.strftime('%d/%m/%y')

### Date of Birth
# Convert to datetime
df_raw_ref['Date of Birth'] = pd.to_datetime(df_raw_ref['Date of Birth'], format = '%Y-%m-%d')

# Format
df_raw_ref['Date of Birth'] = df_raw_ref['Date of Birth'].dt.strftime('%d/%m/%y')


##### Step6: Reorder columns
list_col_upload =   [
                    'Username'
                    ,'First Name'
                    ,'Last Name'
                    ,'Primary Email'
                    ,'Password'
                    ,'Language'
                    ,'Training Academy Department'
                    ,'Level'
                    ,'Default Jobs (HR)'
                    ,'Store Manager Employee Id'
                    ,'Branch Name'
                    ,'Branch Code'
                    ,'Store Name'
                    ,'Store Code'
                    ,'Store Address'
                    ,'City'
                    ,'State'
                    ,'Zipcode'
                    ,'Country'
                    ,'Phone'
                    ,'Timezone'
                    ,'Date of Birth'
                    ]

df_raw_ref = df_raw_ref[list_col_upload]

### Convert names as only first letter capital. Add on 23rd SEP
# title() method is a string method, and DataFrame columns are pandas Series objects.
# To apply the title() method to each element in the column, you need to use the .apply() method.
df_raw_ref['First Name'] = df_raw_ref['First Name'].apply(lambda x: x.title())
df_raw_ref['Last Name'] = df_raw_ref['Last Name'].apply(lambda x: x.title())

##### Step7: Output files
# Local path
path_upload = r"C:\Users\\Desktop\LightYearAutomation\03.UploadFiles\\"
path_out = path_upload + today_str + "-Employee_Upload" + fmt_csv
print(path_out)

df_raw_ref.to_csv(path_out, index=False)

# R drive path
fld_upload = r"C:\Data\Administration\RMG\HR\ Academy\05.UploadFiles\\"
fld_upload_R = fld_upload + today_str + "-Employee_Upload" + fmt_csv
print(fld_upload_R)

df_raw_ref.to_csv(fld_upload_R, index=False)

