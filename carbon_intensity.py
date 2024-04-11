import unittest
import sqlite3
import requests
import json
import os

#import carbon intensity API

def api_request(date):
    '''
    Arguments: Takes a date in string form in the format YYYY-MM-DD
    
    Returns: The data from the website for that day in 30 minute intervals
    '''

    headers = {'Accept': 'application/json'}
    from_ = f"{date}T00:30Z"
    to = f"{date}T23:59Z"
    r = requests.get(f'https://api.carbonintensity.org.uk/regional/intensity/{from_}/{to}/regionid/13', params={}, headers = headers).json()
    # print(r)
    return r

# #creating database
# def set_up_database():
#     path = os.path.dirname(os.path.abspath(__file__))
#     conn = sqlite3.connect(path + "/" + 'carbon_intensity.db')
#     cur = conn.cursor()
#     return cur, conn

def create_table(r, cur, conn):
    for i in range(len(r['data']['data'])):
        cur.execute("DROP TABLE IF EXISTS Carbon_Intensity_Data_April9")
        cur.execute("CREATE TABLE IF NOT EXISTS Carbon_Intensity_Data_April9 (start_time TEXT PRIMARY KEY, intensity_forecast TEXT)")

    #initializing variables that will go into database
    for interval in r['data']['data']: 
        # print(interval)
        start_time = interval['from'][-6:-1]
        # print(start_time)
        end_time = interval['to'][-6:-1]
        # print(end_time)
        intensity_forecast = interval['intensity']['forecast']
        # print(intensity_forecast)
        cur.execute("INSERT OR IGNORE INTO Carbon_Intensity_Data_April9 (start_time, intensity_forecast) VALUES (?,?)", (start_time, intensity_forecast))

    conn.commit()

def main():
    r = api_request('2024-04-09')
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + 'carbon_intensity.db')
    cur = conn.cursor()
    create_table(r, cur, conn)


