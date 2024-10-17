# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 11:17:05 2024

@author: donar
"""

import requests
import pandas as pd

def get_carriers():

    url = "https://api.truckercloud.com/api/v4/carriers"
    
    headers = {
        "accept": "application/json",
        "Authorization": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJoZG9sIiwiaXNzIjoiaHR0cDovL3d3dy50cnVja2VyY2xvdWQuY29tLyIsImlhdCI6MTcyNjcyNDU4NCwiZXhwIjoxNzI2ODEwOTg0fQ.dMiAzm8IKXDJdQ2CGKQBfwNvScR_68T_AcFFumWprjk"
    }
    
    response = requests.get(url, headers=headers)
    
    print(response.text)
    df_get_carriers=pd.read_json(response.text)
    return df_get_carriers
df_get_carriers=get_carriers()
#df_get_carriers.to_excel(r"D:\hdo\Trucker cloud_carrierlevel\get_carriers.xlsx", index=False)