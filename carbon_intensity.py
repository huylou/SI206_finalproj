import unittest
import sqlite3
import requests
import json
import os

#import carbon intensity API
headers = {'Accept': 'application/json'}
r = requests.get('https://api.carbonintensity.org.uk/regional/regionid/13', params={}, headers = headers).json()
print(r)


