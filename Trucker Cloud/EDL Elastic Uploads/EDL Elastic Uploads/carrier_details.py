# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 12:29:44 2024

@author: shrihari
"""

import requests
import pandas as pd
import credentials as crd
from authentication import get_authtoken

url = crd.carrier_url
authToken = get_authtoken()
headers = {
    "accept": "application/json",
    "Authorization": authToken
}

def get_carrier_data(url, headers):
    carriers = []
    current_page = 0
    page_size = 100

    while True:
        # Update the URL with the current page and size parameters
        paginated_url = f"{url}?page={current_page}&size={page_size}"
        print(paginated_url)
        # Make the API request
        response = requests.get(paginated_url, headers=headers)
        print(response.text)
        data = response.json()

        # Collect the carrier data
        carriers.extend(data['content'])

        # Check if there are more pages
        if current_page >= data['Pagination']['totalPages']:
            break

        current_page += 1

    return carriers

# Get the carrier data
carriers = get_carrier_data(url, headers)

# Extract relevant information from nested structures
carrier_data = []
for carrier in carriers:
    carrier_info = carrier['carrierInfo']
    contact_info = carrier.get('contactInfo', {})
    eld_vendor_info = carrier.get('eldVendorInfo', [{}])[0]

    carrier_data.append({
        'Carrier ID': carrier_info['carrierId'],
        'Carrier Name': carrier_info['carrierName'],
        'First Name': carrier_info['firstName'],
        'Last Name': carrier_info['lastName'],
        'Email': contact_info.get('email', ''),
        'ELD Vendor': eld_vendor_info.get('eldVendor', ''),
        'API Key': eld_vendor_info.get('apiKey', ''),
        'Status': eld_vendor_info.get('status', '')
    })

# Create a DataFrame
df = pd.DataFrame(carrier_data)

# Save to Excel
df.to_excel('Input/carriers_data.xlsx', index=False)

print('Data has been written to carriers_data.xlsx')
