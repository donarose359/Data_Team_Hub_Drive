# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 14:56:10 2024

@author: shrihari
"""


import pandas as pd
from authentication import get_authtoken
import requests
from flatten_data import flatten_dict

df=pd.read_excel('Input/carriers.xlsx')
ddf = df[df['codeType'] == 'DOT']

authToken = get_authtoken()
headers = {
    "accept": "*/*",
    "Authorization": authToken
}

# vehicles_url_pages = "https://api.truckercloud.com/api/v4/vehicles?carrierCode="+ddf['codeType']+"&codeType=DOT&eldVendor="+ddf['eldVendor']+"&page=1&size=3"

flat_vehicles_list = []
for c in range(len(ddf)):#looping through carriers
    vehicles_url = "https://api.truckercloud.com/api/v4/vehicles?carrierCode="+str(ddf['carrierCode'].iloc[0])+"&codeType=DOT&eldVendor="+str(ddf['eldVendor'].iloc[0])+"&page=1&size=100"
    response = requests.get(vehicles_url, headers=headers).json()
    total_pages = response['Pagination']['totalPages']
    
    for p in range(total_pages):
        vehicles_url = "https://api.truckercloud.com/api/v4/vehicles?carrierCode="+str(ddf['carrierCode'].iloc[0])+"&codeType=DOT&eldVendor="+str(ddf['eldVendor'].iloc[0])+"&page="+str(p+1)+"&size=100"
        response = requests.get(vehicles_url, headers=headers).json()
        vehicles_li = response['content']
        print(response['Pagination']['size'])
        for item in vehicles_li:
            flat_data = flatten_dict(item)
            flat_vehicles_list.append(flat_data)
            
            
        # Create DataFrame from flattened data
df = pd.DataFrame(flat_vehicles_list)
df.to_excel('Input/Vehicles.xlsx',index=False)