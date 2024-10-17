# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 11:39:33 2024

@author: donar
"""

import requests
import pandas as pd

def post_carriers_invite():

    url = "https://api.truckercloud.com/api/v4/carriers/invite"
    
    payload = {
        "carrierInfo": {
            "carrierName": "AMERICAN TRANSPORTATION",
            "codeType": "DOT",
            "codeValue": "1151228"
        },
        "contactInfo": {
            "email": "sam+135409335@r1vs.com",
            "firstName": "Dan",
            "lastName": "Wilson"
        }
    }
    headers = {
        "accept": "application/json",
        "Authorization": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJoZG9sIiwiaXNzIjoiaHR0cDovL3d3dy50cnVja2VyY2xvdWQuY29tLyIsImlhdCI6MTcyNjcyNDU4NCwiZXhwIjoxNzI2ODEwOTg0fQ.dMiAzm8IKXDJdQ2CGKQBfwNvScR_68T_AcFFumWprjk",
        "content-type": "application/json"
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    print(response.text)
    df_post_carriers_invite=pd.read_json(response.text)
    return df_post_carriers_invite
df_post_carriers_invite=post_carriers_invite()
#df_post_carriers_invite.to_excel(r"D:\hdo\Trucker cloud_carrierlevel\post_carriers_invite.xlsx", index=False)
