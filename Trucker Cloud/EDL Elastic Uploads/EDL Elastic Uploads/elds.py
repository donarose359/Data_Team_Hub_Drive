# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 12:12:13 2024

@author: shrihari
"""

import requests
from authentication import get_authtoken
from flatten_data import flatten_dict
import pandas as pd

elds_url = "https://api.truckercloud.com/api/v4/elds?page=1&size=100"
authToken = get_authtoken()

headers = {
    "accept": "*/*",
    "Authorization": authToken
}

response = requests.get(elds_url, headers=headers).json()

total_pages = response['Pagination']['totalPages']

elds_li = []
for p in range(total_pages):
    elds_url = "https://api.truckercloud.com/api/v4/elds?page="+str(p)+"&size=100"
    response = requests.get(elds_url, headers=headers).json()
    elds = response['content']
    
    for item in elds:
        flat_data = flatten_dict(item)
        elds_li.append(flat_data)
        
df = pd.DataFrame(elds_li)
df.to_excel('Input/elds.xlsx',index=False)
    
    
