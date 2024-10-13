# -----------------
# This script downloads the CSV files from https://www.vranov.sk/Transparentne-mesto/
# and saves them to the same directory as the script.
#
# Usage:
# - Run the script with `python vranov_sk.py`
# - -m argument merges all CSV files into one
# - -mc argument merges all CSV files into one and cleans up afterwards
#
# Lib requirements:
# - requests
#
# pip install requests 
# -----------------

import os # Import the os module
import requests # Import the HTTP requests module
from concurrent.futures import ThreadPoolExecutor, as_completed

# Definitions
url = 'https://www.vranov.sk/Transparentne-mesto/strana-{page}.html?&action=export'
from_page = 1 # Start page
to_page = 20 # End page

folder_name = 'vranov_sk_csv' # Folder for the CSV files
saved_file_name = 'vranov_sk_page-{page}.csv' # File name for the CSV files
max_workers = 10 # Number of threads to use for downloading

# Create a folder to store the CSV files
if not os.path.exists(folder_name):
	os.makedirs(folder_name)

print(f'\nDownloading files from vranov.sk\t pages {from_page} - {to_page}')
print(f'Dest folder: \t file://{os.path.abspath(folder_name)}')
# Header
print('Counter\tStatus\t\tFile name') 


# --- Main script --- #
def download_page(page):	# Function to download and save a single page
	formatted_url = url.format(page=page) # Format the URL with the page number
	response = requests.get(formatted_url) # Send a GET request to the URL
	
	
	if response.status_code == 200: # 200 = OK
		file_name = saved_file_name.format(page=page) # Name of the file to save
		saved_file_path = os.path.join(folder_name, file_name) # Path to save the file
		
		with open(saved_file_path, 'w', encoding='utf-8') as file: 
			file.write(response.text) 
		
		# Return the status of the download
		return (True, file_name)  # Return success status and file name
	else:
		return (False, formatted_url)  # Return error status and URL	

# Keep track of the number of downloaded files
download_count = 0

# Use ThreadPoolExecutor for concurrent downloads
with ThreadPoolExecutor(max_workers) as executor:
	futures = {executor.submit(download_page, page): page for page in range(from_page, to_page + 1)}
		
	# Print the status of each download as it completes
	for future in as_completed(futures):
		download_count += 1  # Increment the download count
		success, info = future.result()  # Get the result of the download

		if success:
			print(f'{download_count}\tOK\t\t{info}')  # Print success
		else:
			print(f'{download_count}\tERR\t\t{info}')  # Print error
