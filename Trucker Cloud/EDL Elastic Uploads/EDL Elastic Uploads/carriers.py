# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 12:20:36 2024

@author: shrihari
"""

import requests
import credentials as crd
from authentication import get_authtoken
import pandas as pd 
import os
import logging
import datetime

# Configure logging
today_date = datetime.datetime.now().strftime("%Y-%m-%d")
timestamp = datetime.datetime.now().strftime("%H-%M-%S")
log_filename = f"logs_{today_date}_{timestamp}.log"

logging.basicConfig(filename=r'logs\\'+log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

url = crd.carrier_url
authToken = get_authtoken()
headers = {
    "accept": "application/json",
    "Authorization": authToken}

response = requests.get(url, headers=headers).json()
total_pages = response['Pagination']['totalPages']

logger.info(f"API request initiated to {url}\n\nResponse\n\n{response}")  # Log API request


# Initialize an empty list to store extracted data from all pages
extracted_data_all_pages = []

for p in range(total_pages):  # looping through pages
    url = crd.carrier_url + "?page=" + str(p) + "&size=100"

    headers = {
        "accept": "application/json",
        "Authorization": authToken
    }

    response = requests.get(url, headers=headers).json()
    response_content = response['content']
    extracted_data = []

    for c in range(len(response_content)):  # loop through carriers
        carrier_info = response_content[c].get('carrierInfo', {})
        contact_info = response_content[c].get('contactInfo', {})
        eld_vendor_info = response_content[c].get('eldVendorInfo', [{}])[0]

        for code in carrier_info['carrierInfoCodes']:
            flat_data = {
                'carrierId': carrier_info['carrierId'],
                'carrierName': carrier_info['carrierName'],
                'firstName': carrier_info['firstName'],
                'lastName': carrier_info['lastName'],
                'carrierCode': code['carrierCode'],
                'codeType': code['codeType'],
                'email': contact_info['email'],
                'eldVendor': eld_vendor_info['eldVendor'],
                'apiKey': eld_vendor_info['apiKey'],
                'status': eld_vendor_info['status']
            }
    
            extracted_data.append(flat_data)
            
            # Add any additional fields dynamically
            for key, value in carrier_info.items():
                if key not in flat_data:
                    flat_data[key] = value
            for key, value in contact_info.items():
                if key not in flat_data:
                    flat_data[key] = value
            for key, value in eld_vendor_info.items():
                if key not in flat_data:
                    flat_data[key] = value

    # Append extracted data from current page to the list for all pages
    extracted_data_all_pages.extend(extracted_data)

# Creating a DataFrame outside the loop
df = pd.DataFrame(extracted_data_all_pages)
            
if not os.path.exists("Input"):
  # Create the directory if it doesn't exist
  os.makedirs("Input")
df.to_excel('Input/carriers.xlsx',index=False)
logger.info('\nCarrier file created')
        
        
