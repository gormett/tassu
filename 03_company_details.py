import requests
import pandas as pd
import csv
import time
import os

# API base URL
api_url_template = "https://api.statistics.sk/rpo/v1/entity/{company_id}"

# File paths
input_file = 'temp/company_ids.csv'
people_output_file = 'output/people.csv'
company_output_file = 'output/company_details.csv'

# Read unique company IDs from company_ids.csv
with open(input_file, 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='|')
    unique_ids = {row['id'] for row in reader}  # Set of unique IDs to avoid duplicates


# Prepare the output files with headers
with open(company_output_file, 'w', newline='', encoding='utf-8') as company_csv, \
     open(people_output_file, 'w', newline='', encoding='utf-8') as people_csv:

    # Define headers for company and people files
    company_headers = [
        'id', 'identifier', 'fullName', 'otherNames', 'legalForm', 'predecessor', 'successor',
        'address', 'house_number', 'street', 'municipality', 'postalCode', 'country',
        'establishmentDate', 'terminationDate'
    ]
    people_headers = [
        'company_id', 'relationship_type', 'name', 'surname', 'fullName', 'address',
        'house_number', 'street', 'municipality', 'postalCode', 'country', 'role', 'share_percentage',
        'deposit_value', 'start_date', 'end_date'
    ]

    # Create writers
    company_writer = csv.DictWriter(company_csv, fieldnames=company_headers, delimiter='|')
    people_writer = csv.DictWriter(people_csv, fieldnames=people_headers, delimiter='|')


    # Write headers
    company_writer.writeheader()
    people_writer.writeheader()

       # Function to fetch data from API for each company ID
    def fetch_company_data(company_id):
        url = api_url_template.format(company_id=company_id)
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data for ID {company_id}: {e}")
            return None

    # Function to parse and save company data
    def save_company_data(data):
    # Safe extraction with fallback for all fields
        company = {
            'id': data.get('id', ''),
            'identifier': data.get('identifiers', [{}])[0].get('value', '') if data.get('identifiers') else '',
            'fullName': data.get('fullNames', [{}])[0].get('value', '') if data.get('fullNames') else '',
            'otherNames': ", ".join(name.get('value', '') for name in data.get('otherNames', [])) if data.get('otherNames') else '',
            
            # Safe extraction of legal form details with nested fallback
            'legalForm': data.get('legalForms', [{}])[0].get('value', {}).get('value', '') if data.get('legalForms') else '',
            
            # Predecessor and successor fallback
            'predecessor': data.get('predecessor', {}).get('value', '') if data.get('predecessor') else '',
            'successor': data.get('successor', {}).get('value', '') if data.get('successor') else '',
            
            # Address and nested address fields
            'address': data.get('addresses', [{}])[0].get('formatedAddress', '') if data.get('addresses') else '',
            'house_number': data.get('addresses', [{}])[0].get('houseNumber', '') if data.get('addresses') else '',
            'street': data.get('addresses', [{}])[0].get('street', '') if data.get('addresses') else '',
            'municipality': data.get('addresses', [{}])[0].get('municipality', {}).get('value', '') if data.get('addresses') else '',
            'postalCode': data.get('addresses', [{}])[0].get('postalCode', '') if data.get('addresses') else '',
            'country': data.get('addresses', [{}])[0].get('country', {}).get('value', '') if data.get('addresses') else '',
            
            # Dates
            'establishmentDate': data.get('establishment', ''),
            'terminationDate': data.get('termination', '')
        }
        company_writer.writerow(company)

    # Function to parse and save people data (stakeholders, statutoryBodies, shares, deposits, kuvPersonsInfo)
    def save_people_data(data, company_id):
        def extract_person_info(person, relationship_type, role=''):
            address = person.get('address', {})
            person_info = {
                'company_id': company_id,
                'relationship_type': relationship_type,
                'name': person.get('personName', {}).get('givenName', '') if person.get('personName') else '',
                'surname': person.get('personName', {}).get('familyName', '') if person.get('personName') else '',
                'fullName': person.get('personName', {}).get('formatedName', '') if person.get('personName') else '',
                
                # Address fields
                'address': address.get('formatedAddress', ''),
                'house_number': address.get('houseNumber', ''),
                'street': address.get('street', ''),
                'municipality': address.get('municipality', {}).get('value', '') if address.get('municipality') else '',
                'postalCode': address.get('postalCode', ''),
                'country': address.get('country', {}).get('value', '') if address.get('country') else '',
                
                # Role, share, deposit, and dates
                'role': role or person.get('stakeholderType', {}).get('value', '') if person.get('stakeholderType') else '',
                'share_percentage': person.get('share', '') if 'share' in person else '',
                'deposit_value': person.get('deposit', '') if 'deposit' in person else '',
                'start_date': person.get('validFrom', ''),
                'end_date': person.get('validTo', '')
            }
            people_writer.writerow(person_info)

        # Process statutory bodies
        for body in data.get('statutoryBodies', []):
            extract_person_info(body, 'statutoryBody', body.get('stakeholderType', {}).get('value', '') if body.get('stakeholderType') else '')

        # Process shares
        for share in data.get('shares', []):
            extract_person_info(share, 'shareholder')

        # Process deposits
        for deposit in data.get('deposits', []):
            extract_person_info(deposit, 'depositor')

        # Process kuvPersonsInfo
        for kuv_person in data.get('kuvPersonsInfo', []):
            extract_person_info(kuv_person, 'kuvPerson')

    # Iterate through unique company IDs, fetch data, and save to CSVs
    for idx, company_id in enumerate(unique_ids, start=1):
        # Fetch data from API
        data = fetch_company_data(company_id)
        if data:
            # Save data for company and people
            save_company_data(data)
            save_people_data(data, company_id)
            print(f"Processed company ID {company_id} ({idx}/{len(unique_ids)})")

        time.sleep(2)

print("Data processing completed.")
