import csv
import requests
import time
import os


# Ensure 'temp' directory exists
os.makedirs('temp', exist_ok=True)


# Define the API URL template
api_url_template = "https://api.statistics.sk/rpo/v1/search?identifier={ico}"

# File paths
input_file = 'output/contract_details.csv'
output_file = 'temp/company_ids.csv'

# Read the CSV file and extract Dodávateľ IČO values
ico_set = set()
print("Reading ICOs from contract_links.csv...")
with open(input_file, 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='|')
    for row in reader:
        ico = row.get('Dodávateľ IČO')
        if ico:
            ico_set.add(ico)

print(f"Found {len(ico_set)} unique ICOs.")

# Remove duplicates
# ico_list = list(set(ico_list))
# print(f"Removed duplicates. {len(ico_list)} unique ICOs remain.")

# Function to fetch data from the API
def fetch_ico_data(ico):
    url = api_url_template.format(ico=ico)
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"Error fetching data for ICO {ico}: {e}")
        return None

# Function to clean and flatten the data
def clean_data(result, ico):
    return {
        'identifier': ico,
        'id': result.get('id'),
        'fullName_value': result['fullNames'][0]['value'] if result.get('fullNames') else '',
    }

# Prepare to write to the output CSV file
with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['identifier', 'id', 'fullName_value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='|')

    # Write header only if the file is empty
    if csvfile.tell() == 0:
        writer.writeheader()

    # Process each ICO, fetching and writing data
    for index, ico in enumerate(ico_set, start=1):
        data = fetch_ico_data(ico)
        if data and 'results' in data and data['results']:
            result = data['results'][0]  # Assuming the first result is desired
            cleaned_data = clean_data(result, ico)
            writer.writerow(cleaned_data)
            print(f"Processed ICO {index}/{len(ico_set)}: {ico} - Success")
        else:
            print(f"Processed ICO {index}/{len(ico_set)}: {ico} - No Data")

        csvfile.flush()
        time.sleep(2)

print("Data fetching and writing completed.")