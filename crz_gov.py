import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import csv

# Step 1: Load the CSV with list of customers
df = pd.read_csv('obce_VT.csv')

# Step 2: Define rate limiting function
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
def get_contracts_links(customer):
		base_url = f"https://crz.gov.sk/2171273-sk/centralny-register-zmluv/?art_zs1=Obec+{customer}"
		page = 0
		contracts_links = []
		request_count = 0
		start_time = time.time()

		print(f"\n{customer}\t\t", end=' ')
		while True:
				# Append the page number to the base URL
				paginated_url = f"{base_url}&page={page}"
				# print(f"Page \t{page}")
				
				# Apply rate limiting
				rate_limit(request_count, start_time)
				request_count += 1
				
				try:
						response = requests.get(paginated_url)
						response.raise_for_status()  # Raise exception if request fails
				except requests.RequestException as e:
						print(f"Error fetching page {page} for customer {customer}: {e}")
						break
				
				soup = BeautifulSoup(response.text, 'html.parser')

				# Correct selector for the Zmluva links
				# links = soup.select('td.cell2 a[href^="/zmluva/"], a[href^="https://crz.gov.sk/"]')  # Updated selector to find Zmluva links
				links = soup.select('td.cell2 a')  # Selector to find links in cell2
				print(f"Page\t{page}\tLinks\t{len(links)}\t\t{paginated_url}")

				if len(links) == 0:
						# print(f"No contracts found on page {page} for {customer}, stopping.")
						break

				# Add the full links to the list
				contracts_links.extend([f"https://crz.gov.sk{link['href']}" for link in links])

				# Write the links to a links.csv file with | delimiter, customer and link
				with open('links.csv', 'a', newline='', encoding='utf-8') as f:
						writer = csv.writer(f, delimiter='|')
						writer.writerows([[customer, link] for link in contracts_links])
						
				# Check for the 'Next' button (adjust class if necessary)
				next_page_button = soup.find('a', class_='page-link page-link---next')

				# If no 'Next' button, we are at the last page
				if not next_page_button:
						print(f"No 'Next' button found on page {page}. Finished scraping for {customer}.")
						break

				# Move to the next page
				page += 1

		return contracts_links

# Step 4: Iterate over each customer and store the results
all_customers_data = []

for customer in df['Obec']:
	contracts_links = get_contracts_links(customer)
	if contracts_links:
		for link in contracts_links:
			all_customers_data.append({'Obec': customer, 'contract_link': link})

# Save all customers' contract links to a CSV file with pipe delimiter
all_customers_df = pd.DataFrame(all_customers_data)
all_customers_df.to_csv('all_links_to_contracts.csv', index=False, sep='|')
print(f"Saved all customers' contract links to all_links_to_contracts.csv")


# Step 5: Iterate over each contract link and scrape the contract details
def scrape_contract_details(contract_link):
			# Load the CSV with all contract links
	all_links_df = pd.read_csv('all_links_to_contracts.csv', delimiter='|')
	
	for index, row in all_links_df.iterrows():
		contract_link = row['contract_link']
		print(f"Scraping contract details for: {contract_link}")
		request_count = 0
		start_time = time.time()
		
		

		# Apply rate limiting
		rate_limit(request_count, start_time)
		request_count += 1

		try:
				response = requests.get(contract_link)
				response.raise_for_status()  # Raise exception if request fails
		except requests.RequestException as e:
				print(f"Error fetching contract details for {contract_link}: {e}")
				return None

		response_text = response.text
		
		soup = BeautifulSoup(response_text, 'html.parser')
							
		# Extract contract details
		contract_details = {}

		# Extract contract details
		contract_details['Typ'] = soup.find('strong', string='Typ:').find_next('span').text
		contract_details['Č. zmluvy'] = soup.find('strong', string='Č. zmluvy:').find_next('span').text
		contract_details['Rezort'] = soup.find('strong', string='Rezort:').find_next('span').text
		contract_details['Objednávateľ'] = soup.find('strong', string='Objednávateľ:').find_next('span').get_text(separator=", ").strip()
		contract_details['Objednávateľ IČO'] = soup.find('strong', string='IČO:').find_next('span').text
		contract_details['Dodávateľ'] = soup.find('strong', string='Dodávateľ:').find_next('span').get_text(separator=", ").strip()
		contract_details['Dodávateľ IČO'] = soup.find_all('strong', string='IČO:')[1].find_next('span').text  # Second occurrence of IČO
		contract_details['Názov zmluvy'] = soup.find('strong', string='Názov zmluvy:').find_next('span').text
		contract_details['ID zmluvy'] = soup.find('strong', string='ID zmluvy:').find_next('span').text
		
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
		
		return contract_details

# Step 6: Iterate over all contract links and scrape the details and save the detials to a CSV file
all_contracts_data = []

for index, row in all_customers_df.iterrows():
		link = row['contract_link']
		obec = row['Obec']
		contract_details = scrape_contract_details(link)
		if contract_details:
				contract_details['Obec'] = obec
				contract_details['Link'] = link
				all_contracts_data.append(contract_details)

# Define the headers for the CSV file
headers = [
		'Obec', 'Link', 'Typ', 'Č. zmluvy', 'Rezort', 'Objednávateľ', 'Objednávateľ IČO', 
		'Dodávateľ', 'Dodávateľ IČO', 'Názov zmluvy', 'ID zmluvy', 'Dátum zverejnenia', 
		'Dátum uzavretia', 'Dátum účinnosti', 'Dátum platnosti do', 'Zmluvne dohodnutá čiastka', 
		'Celková čiastka'
]

# Save all contracts' details to a CSV file with pipe delimiter
all_contracts_df = pd.DataFrame(all_contracts_data)
all_contracts_df = all_contracts_df.reindex(columns=headers)
all_contracts_df.to_csv('all_contracts_data.csv', index=False, sep='|')

print(f"Saved all contracts' details to all_contracts_data.csv")