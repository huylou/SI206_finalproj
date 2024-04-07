import unittest
import sqlite3
import requests
import json
import os
import re
import matplotlib.pyplot as plt

#import carbon intensity API
headers = {'Accept': 'application/json'}
r = requests.get('https://api.carbonintensity.org.uk/intensity/date', params={}, headers = headers)


