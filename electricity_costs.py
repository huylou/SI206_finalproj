import unittest
import sqlite3
import requests
import json
import os

dno = 'dno=12'
voltage = 'voltage=LV'
start = 'start=09-04-2024'
end = 'end=10-04-2024'
r = requests.get(f'https://odegdcpnma.execute-api.eu-west-2.amazonaws.com/development/prices?{dno}&{voltage}&{start}&{end}').json()
print(r)