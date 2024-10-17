# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 16:11:04 2024

@author: shrihari
"""

import pandas as pd
from authentication import get_authtoken
import requests

df=pd.read_excel('Output/carriers.xlsx')
ddf = df[df['codeType'] == 'DOT']

authToken = get_authtoken()
headers = {
    "accept": "*/*",
    "Authorization": authToken
}

# drivers_url_pages = "https://api.truckercloud.com/api/v4/drivers?carrierCode="+ddf['codeType']+"&codeType=DOT&eldVendor="+ddf['eldVendor']+"&page=1&size=3"

for c in range(len(ddf)):#looping through carriers
    drivers_url = "https://api.truckercloud.com/api/v4/drivers?carrierCode="+str(ddf['carrierCode'].iloc[0])+"&codeType=DOT&eldVendor="+str(ddf['eldVendor'].iloc[0])+"&page=1&size=100"
    response = requests.get(drivers_url, headers=headers).json()
    total_pages = response['Pagination']['totalPages']
    flat_drivers_list = []
    for p in range(total_pages):
        drivers_url = "https://api.truckercloud.com/api/v4/drivers?carrierCode="+str(ddf['carrierCode'].iloc[0])+"&codeType=DOT&eldVendor="+str(ddf['eldVendor'].iloc[0])+"&page="+str(p+1)+"&size=100"
        response = requests.get(drivers_url, headers=headers).json()
        drivers_li = response['content']
        
        for item in drivers_li:
            flat_data = {}
            for key, value in item.items():
                if isinstance(value, list):
                    for sub_dict in value:
                        flat_data.update(sub_dict)
                else:
                    flat_data[key] = value
            flat_drivers_list.append(flat_data)
        
        # Create DataFrame from flattened data
        df = pd.DataFrame(flat_drivers_list)
df.to_excel('Input/drivers.xlsx',index=False)