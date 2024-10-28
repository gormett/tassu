import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import csv
import os

choice_map = {
  '1': 'links',
  '2': 'details',
  '3': 'both'
}

choice = input("Enter '1' to scrape links, '2' to scrape contract details, or '3' to run both: ").strip()
choice = choice_map.get(choice, 'both')

# Step 1: Load the CSV with list of municipalities
df = pd.read_csv('obce_VT.csv')

os.makedirs('temp', exist_ok=True)
os.makedirs('output', exist_ok=True)

links_csv_path = 'temp/contract_links.csv'
details_csv_path = 'output/contract_details.csv'

def save_links(links, objednavatel):
    file_exists = os.path.exists(links_csv_path)
    with open(links_csv_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='|')

        if not file_exists or os.path.getsize(links_csv_path) == 0:
            writer.writerow(['Link', 'Obec'])
        writer.writerows([[objednavatel, link] for link in links])


# Define the headers for the CSV file
headers = [
    'Obec', 'Link', 'Typ', 'Č. zmluvy', 'Rezort', 'Objednávateľ', 'Objednávateľ IČO',
    'Dodávateľ', 'Dodávateľ IČO', 'Názov zmluvy', 'ID zmluvy', 'Zverejnil', 'Verejné obstarávanie', 'Dátum zverejnenia',
    'Dátum uzavretia', 'Dátum účinnosti', 'Dátum platnosti do', 'Zmluvne dohodnutá čiastka',
    'Celková čiastka'
]

def save_contract_details(details):
    # Write headers if file does not exist
    if not os.path.exists(details_csv_path):
        with open(details_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='|')
            writer.writerow(headers)
    # Write contract details to output folder
    with open(details_csv_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerow([details.get(header, '') for header in headers])

# Step 2: Define rate limiting function
# https://www.crz.gov.sk/stahovanie-udajov-z-crz/
def rate_limit(request_count, start_time):
    current_time = datetime.now().time()
    if current_time >= datetime.strptime("06:00", "%H:%M").time() and current_time <= datetime.strptime("20:00", "%H:%M").time():
        # Daytime limits
        if request_count % 50 == 0:
            print(f"Rate limiting: Pausing for 5 seconds.")
            time.sleep(5)
        elif request_count % 30 == 0:
            print(f"Rate limiting: Pausing for 10 seconds.")
            time.sleep(10)
        elif request_count % 60 == 0:
            print(f"Rate limiting: Pausing for 60 seconds.")
            time.sleep(60)
        elif request_count % 80 == 0:
            print(f"Rate limiting: Pausing for 120 seconds.")
            time.sleep(120)
    else:
        # Nighttime limits
        if request_count % 50 == 0:
            print(f"Rate limiting: Pausing for 5 seconds (nighttime).")
            time.sleep(5)
        elif request_count % 30 == 0:
            print(f"Rate limiting: Pausing for 10 seconds (nighttime).")
            time.sleep(10)

# Step 3: Define function to get contracts links for multiple pages
def get_contracts_links(objednavatel):
    base_url = f"https://crz.gov.sk/2171273-sk/centralny-register-zmluv/?art_zs1={objednavatel}"
    page = 0 # CRZ uses 0-based indexing for pagination
    contracts_links = []
    request_count = 0
    start_time = time.time()

    print(f"\n{objednavatel}\t\t", end=' ')
    while True:
        paginated_url = f"{base_url}&page={page}"

        rate_limit(request_count, start_time)
        request_count += 1

        try:
            response = requests.get(paginated_url)
            response.raise_for_status()  # Raise exception if request fails
        except requests.RequestException as e:
            print(f"Error fetching page {page} for customer {objednavatel}: {e}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')

        # Correct selector for the Zmluva links
        links = soup.select('td.cell2 a')  # Selector to find links in cell2
        print(f"Page\t{page}\tLinks\t{len(links)}\t\t{paginated_url}")

        if len(links) == 0:
            # print(f"No contracts found on page {page} for {customer}, stopping.")
            break

        # Add the full links to the list
        contracts_links.extend([f"https://crz.gov.sk{link['href']}" for link in links])

        # Check for the 'Next' button (adjust class if necessary)
        next_page_button = soup.find('a', class_='page-link page-link---next')

        # If no 'Next' button, we are at the last page
        if not next_page_button:
            print(f"No 'Next' button found on page {page}. Finished scraping for {objednavatel}.")
            break

        # Move to the next page
        page += 1
    save_links(contracts_links, objednavatel)
    return contracts_links

# Step 5: Iterate over each contract link and scrape the contract details
def scrape_contract_details(contract_link, obec):
    request_count = 0
    start_time = time.time()
    try:
        response = requests.get(contract_link)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching contract details for {contract_link}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    cleaned_obec = obec.replace('+', ' ')   # Replace '+' with space in the municipality name

    contract_details = {
        'Obec': cleaned_obec,
        'Link': contract_link,
    }

    # Extract contract details
    contract_details['Typ'] = soup.find('strong', string='Typ:').find_next('span').text
    
    contract_number_element = soup.find('strong', string='Č. zmluvy:')
    if contract_number_element:
        contract_details['Č. zmluvy'] = contract_number_element.find_next('span').text
    else:
        contract_details['Č. zmluvy'] = None
    
    contract_details['Rezort'] = soup.find('strong', string='Rezort:').find_next('span').text
    contract_details['Objednávateľ'] = soup.find('strong', string='Objednávateľ:').find_next('span').get_text(separator=", ").strip()
    contract_details['Dodávateľ'] = soup.find('strong', string='Dodávateľ:').find_next('span').get_text(separator=", ").strip()
    
    ico_elements = soup.find_all('strong', string='IČO:')
    if len(ico_elements) > 0:
        contract_details['Objednávateľ IČO'] = ico_elements[0].find_next('span').text
    if len(ico_elements) > 1:
        contract_details['Dodávateľ IČO'] = ico_elements[1].find_next('span').text
    else:
        contract_details['Dodávateľ IČO'] = None  # Handle missing second "IČO"


    contract_details['Názov zmluvy'] = soup.find('strong', string='Názov zmluvy:').find_next('span').text
    contract_details['ID zmluvy'] = soup.find('strong', string='ID zmluvy:').find_next('span').text
    contract_details['Zverejnil'] = soup.find('strong', string='Zverejnil:').find_next('span').text
    # Extract Verejné obstarávanie link if it exists
    verejne_obstaravanie_element = soup.find('strong', string='Verejné obstarávanie:')
    if verejne_obstaravanie_element:
        link_tag = verejne_obstaravanie_element.find_next('a')
        if link_tag and 'href' in link_tag.attrs:
            contract_details['Verejné obstarávanie'] = link_tag['href']
        else:
            contract_details['Verejné obstarávanie'] = None

    # Extract date-related details
    contract_details['Dátum zverejnenia'] = soup.find('strong', string='Dátum zverejnenia:').find_next('span').text
    contract_details['Dátum uzavretia'] = soup.find('strong', string='Dátum uzavretia:').find_next('span').text
    contract_details['Dátum účinnosti'] = soup.find('strong', string='Dátum účinnosti:').find_next('span').text
    contract_details['Dátum platnosti do'] = soup.find('strong', string='Dátum platnosti do:').find_next('span').text

    # Extract financial details
    contract_details['Zmluvne dohodnutá čiastka'] = soup.find('span', string='Zmluvne dohodnutá čiastka:').find_next('span').text
    contract_details['Celková čiastka'] = soup.find('strong', string='Celková čiastka:').find_next('strong').text

    # Print the extracted contract details
    for key, value in contract_details.items():
        print(f'\t{key}: \t{value}')

    save_contract_details(contract_details)

    return contract_details


if choice in ('links', 'both'):
    for objednavatel in df['Obec']:
        get_contracts_links(objednavatel)

# Phase 2: Scrape details from saved links
if choice in ('details', 'both'):
    with open(links_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='|')
        request_count = 0
        start_time = time.time()
        for row in reader:
            objednavatel, contract_link = row
            scrape_contract_details(contract_link, objednavatel)
            rate_limit(request_count, start_time)
            request_count += 1


