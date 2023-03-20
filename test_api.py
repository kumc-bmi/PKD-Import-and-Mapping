#!/usr/bin/env python

import requests
data = {
    'token': 'B14F279659FE996C53F4B1EE8AA7A1D8',
    'content': 'user',
    'format': 'json',
    'returnFormat': 'json'
}
r = requests.post('https://redfin.kumc.edu/api/', data=data)
print('HTTP Status: ' + str(r.status_code))
print(r.status_code())

