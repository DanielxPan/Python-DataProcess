
# Delete files in R drive
import os
import datetime

# List of folder paths
folder_paths = [r"C:\Users\Reddrop\Desktop\LightYearAutomation\03.UploadFiles\\"
                ,  r"C:\data\Lightyear\06.ScheduledAutomation\98.ResultsFiles\\"
                , r"C:\data\Lightyear\06.ScheduledAutomation\02.UploadFiles\\"]

# Calculate the date range
today = datetime.date.today()
end_date = today - datetime.timedelta(days=30)  # 30 days ago
start_date = today - datetime.timedelta(days=60)  # 60 days ago
print(end_date)
print(start_date)

# Iterate through each folder path
for folder_path in folder_paths:
    # List files in the folder
    file_list = os.listdir(folder_path)

    # Iterate through the files in the current folder
    for file_name in file_list:
        # Extract the date part of the file name
        date_str = file_name.split('_')[0]
        
        # The try and except blocks in Python are used for error handling
        try:
            # Try functions that could encounter error
            file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Keep executing if try block succeeds
            if start_date <= file_date <= end_date:
                file_path = os.path.join(folder_path, file_name)
                os.remove(file_path)
                print(f"Deleted file: {file_name}")
        
        # Indicate the script what to do next if try block fails
        except ValueError:
            # Skip files with invalid date format
            continue
