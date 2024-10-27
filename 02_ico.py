import csv
import requests
import time

# Define the API URL template
api_url_template = "https://api.statistics.sk/rpo/v1/search?identifier={ico}"

# Read the CSV file and extract Dodávateľ IČO values
ico_list = []
print("Reading ICOs from CSV file...")
with open('all_contracts_ongoing.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='|')
    for row in reader:
        ico = row.get('Dodávateľ IČO')
        if ico:
            ico_list.append(ico)
print(f"Found {len(ico_list)} ICOs.")

# Remove duplicates
ico_list = list(set(ico_list))
print(f"Removed duplicates. {len(ico_list)} unique ICOs remain.")

# Function to fetch data from the API
def fetch_ico_data(ico):
    url = api_url_template.format(ico=ico)
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        # Remove the 'license' field from the response
        if 'license' in data:
            del data['license']
        return data
    except requests.RequestException as e:
        print(f"Error fetching data for IČO {ico}: {e}")
        return None

# Function to clean and flatten the data
def clean_data(result, ico):
    return {
        'ico': ico,
        'id': result.get('id'),
        'fullName_value': result['fullNames'][0]['value'] if result.get('fullNames') else '',
    }

# Prepare to write to the output CSV file
print("Writing data to ico_VT.csv...")
with open('ico_VT.csv', 'a', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['ico', 'id', 'fullName_value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='|')
    
    # Write the header only if the file is empty
    if csvfile.tell() == 0:
        writer.writeheader()

    # Iterate over the ICO list and fetch data
    total_icos = len(ico_list)
    entries_processed = 0
    
    for index, ico in enumerate(ico_list, start=1):
        data = fetch_ico_data(ico)
        
        if data and 'results' in data and data['results']:
            result = data['results'][0]  # Assuming we want the first result
            cleaned_data = clean_data(result, ico)
            writer.writerow(cleaned_data)
            status = "ok"
        else:
            status = "err"
        
        print(f"{index} - {total_icos}\tICO: {ico}\t{status}")
        time.sleep(2)  # Wait for 2 seconds before making the next request

        entries_processed += 1
        if entries_processed % 50 == 0:
            csvfile.flush()
            print(f"Saved {entries_processed} entries to file.")

    # Final flush to ensure all entries are saved
    csvfile.flush()
    print(f"Saved final {entries_processed} entries to file.")
print("Data fetching and writing completed.")