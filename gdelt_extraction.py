import csv
import requests
from datetime import datetime

# URL to master file list
master_file_list_url = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'

# Date range
start_date = datetime(2021, 2, 10)
end_date = datetime.today()

# Download the master file list
response = requests.get(master_file_list_url)
file_list = response.text.splitlines()

# Open a CSV file to save the filtered data
with open('gdelt_filtered_files.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['File Size', 'MD5 Hash', 'URL'])  # Header

    # Filter based on the date range and write to the CSV
    for line in file_list:
        parts = line.split()
        
        # Ensure that there are exactly three parts: file size, MD5 hash, and URL
        if len(parts) != 3:
            continue  # Skip lines that don't follow the expected format

        file_size = parts[0]
        md5_hash = parts[1]
        file_url = parts[2]

        # Extract date from URL in the format YYYYMMDDHHMM
        try:
            file_date_str = file_url.split('/')[-1][:12]
            file_date = datetime.strptime(file_date_str, '%Y%m%d%H%M')
        except ValueError:
            continue  # Skip invalid date formats

        # Filter files by the date range
        if start_date <= file_date <= end_date:
            writer.writerow([file_size, md5_hash, file_url])

print("CSV file 'gdelt_filtered_files.csv' created successfully!")
