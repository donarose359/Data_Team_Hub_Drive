# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 12:13:26 2024

@author: shrihari
"""

import requests
import credentials as crd

def get_authtoken():
    url = crd.auth_url
    
    payload = {
        "userName": crd.usr,
        "password": crd.pwd
    }
    headers = {
        "accept": "*/*",
        "content-type": "application/json"
    }
    
    response = requests.post(url, json=payload, headers=headers)
    resp = response.json()
    authToken = resp['authToken']
    return authToken

