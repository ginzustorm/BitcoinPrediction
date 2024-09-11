import csv
import requests
import zipfile
import os
from io import BytesIO, TextIOWrapper
from datetime import datetime

# Increase the CSV field size limit to a much higher value
csv.field_size_limit(10**9)  # Set to 1 billion characters (adjust if needed)

# Function to download, unzip, and process GKG files for sentiment analysis
def download_unzip_and_process_gkg(file_url):
    # Get the file name from the URL
    file_name = file_url.split('/')[-1]
    
    # Download the zip file
    with requests.get(file_url, stream=True) as response:
        response.raise_for_status()  # Raise an error for bad responses

        # Use BytesIO to handle the zip file in memory
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            # Loop through files in the zip (usually just one CSV)
            for file in z.namelist():
                # Open the CSV file inside the zip
                with z.open(file) as csvfile:
                    # Read the binary content from the CSV file inside the zip
                    content = csvfile.read()

                    # Try utf-8 decoding first, then fallback to ISO-8859-1 if needed
                    try:
                        decoded_content = content.decode('utf-8')
                    except UnicodeDecodeError:
                        decoded_content = content.decode('ISO-8859-1')

                    # Now that we have the decoded content, process it line by line
                    csvreader = csv.reader(decoded_content.splitlines())
                    for row in csvreader:
                        process_gkg_row(row)

# Function to process each GKG row and extract sentiment (Tone)
def process_gkg_row(row):
    # Assuming the relevant fields in the GKG files are:
    # Index 0: Date, Index 7: Tone (these indices may need to be adjusted)
    date_str = row[0]  # Date of the event/article
    try:
        file_year = datetime.strptime(date_str, '%Y%m%d%H%M%S').year
    except ValueError:
        return  # Skip rows that don't have a valid date

    tone_str = row[7]  # Tone of the article
    try:
        tone = float(tone_str)
    except ValueError:
        return  # Skip rows that don't have a valid tone value

    # Create a folder for the year if it doesn't exist
    if not os.path.exists(f'./gkg_data/{file_year}'):
        os.makedirs(f'./gkg_data/{file_year}')
    
    # Append the row to the corresponding yearly dataset
    with open(f'./gkg_data/{file_year}/{file_year}_sentiment_data.csv', 'a', newline='', encoding='utf-8') as year_file:
        writer = csv.writer(year_file)
        writer.writerow([date_str, tone])  # Save only the date and tone for sentiment analysis

# Function to process the list of GKG file URLs and download only files every 4 hours
def process_gkg_file_links(file_links_csv):
    with open(file_links_csv, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        
        # Process only files that have a timestamp where HH is divisible by 4 (every 4 hours)
        for row in reader:
            file_url = row[2]  # Assuming the third column is the URL
            if "gkg.csv.zip" in file_url:  # Filter for GKG files
                timestamp_str = file_url.split('/')[-1][:14]  # Extract YYYYMMDDHHMMSS from the filename
                try:
                    file_time = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
                    # Check if the hour is divisible by 4 (e.g., 00:00:00, 04:00:00, 08:00:00)
                    if file_time.hour % 4 == 0 and file_time.minute == 0 and file_time.second == 0:
                        print(f'Downloading and processing GKG file: {file_url}')
                        download_unzip_and_process_gkg(file_url)
                except ValueError:
                    continue  # Skip files with invalid timestamps

# Run the process (adjust the filename to match your CSV with URLs)
process_gkg_file_links('gdelt_filtered_files.csv')  # The file you created with URLs
