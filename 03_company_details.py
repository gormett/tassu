import requests
import pandas as pd
import csv
import time

# API base URL
API_URL = "https://api.statistics.sk/rpo/v1/entity/"

# Load IDs from ico_VT.csv
ids_df = pd.read_csv('ico_VT.csv', delimiter='|')
ids = ids_df['id'].dropna().unique()  # Remove duplicates and NaN values

# Function to flatten nested fields with specified keys
def flatten_field(field, keys):
    """
    Extracts values of specified keys from a list of dictionaries or nested structures.
    Returns a comma-separated string of values for each specified key.
    """
    if isinstance(field, list):
        return ', '.join([', '.join(str(item.get(key, '')) for key in keys) for item in field])
    elif isinstance(field, dict):
        return ', '.join(str(field.get(key, '')) for key in keys)
    return ''

# Function to fetch and parse details for each ID
def fetch_id_details(id):
    url = f"{API_URL}{id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Prepare flattened details
        id_details = {
            'ID': id,
            'dbModificationDate': data.get('dbModificationDate', ''),
            'identifiers': flatten_field(data.get('identifiers', []), ['value']),
            'fullNames': flatten_field(data.get('fullNames', []), ['value']),
            'alternativeNames': flatten_field(data.get('alternativeNames', []), ['value']),
            'addresses': flatten_field(data.get('addresses', []), ['formatedAddress']),
            'legalForms': flatten_field(data.get('legalForms', []), ['value', 'code', 'codelistCode']),
            'establishment': data.get('establishment', ''),
            'termination': data.get('termination', ''),
            'activities': flatten_field(data.get('activities', []), ['economicActivityDescription']),
            'statutoryBodies': flatten_field(data.get('statutoryBodies', []), ['fullName']),
            'stakeholders': flatten_field(data.get('stakeholders', []), ['stakeholderType']),
            'legalStatuses': flatten_field(data.get('legalStatuses', []), ['value']),
            'otherLegalFacts': flatten_field(data.get('otherLegalFacts', []), ['value']),
            'authorizations': flatten_field(data.get('authorizations', []), ['value']),
            'equities': flatten_field(data.get('equities', []), ['value']),
            'shares': flatten_field(data.get('shares', []), ['fullName']),
            'deposits': flatten_field(data.get('deposits', []), ['type']),
            'sourceRegister': flatten_field(data.get('sourceRegister', {}), ['value']),
            'predecessors': flatten_field(data.get('predecessors', []), ['fullName']),
            'successors': flatten_field(data.get('successors', []), ['fullName']),
            'statisticalCodes': flatten_field(data.get('statisticalCodes', {}).get('mainActivity', {}), ['value']),
            'kuvPersonsInfo': flatten_field(data.get('kuvPersonsInfo', []), ['formatedName']),
            'topManagementsInfo': flatten_field(data.get('topManagementsInfo', []), ['formatedName']),
            'organizationUnits': flatten_field(data.get('organizationUnits', []), ['value']),
        }

        return id_details

    except requests.RequestException as e:
        print(f"Error fetching data for ID {id}: {e}")
        return None

# Write ID details to CSV
output_file = 'ico_details.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = None  # Initialize writer after the first response to define headers

    # Process each ID and fetch details
    for id in ids:
        print(f"Fetching details for ID: {id}")
        details = fetch_id_details(id)
        
        if details:
            # Initialize CSV headers based on keys from the first response
            if writer is None:
                writer = csv.DictWriter(csvfile, fieldnames=details.keys(), delimiter='|')
                writer.writeheader()

            # Write ID details to CSV
            writer.writerow(details)
            print(f"Saved details for ID: {id} to {output_file}")

        # Rate limiting to prevent overwhelming the API
        time.sleep(1)  # 1-second delay between requests
