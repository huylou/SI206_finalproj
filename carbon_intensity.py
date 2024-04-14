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
    '''
    Arguments: takes the json output from the API request
    '''

    # cur.execute("DROP TABLE IF EXISTS Carbon_Intensity_Data")
    cur.execute("CREATE TABLE IF NOT EXISTS Carbon_Intensity_Data (timestamp TEXT PRIMARY KEY, date TEXT, time TEXT, intensity_forecast TEXT)")

    #find existing timestamps in database
    count = 0
    cur.execute("SELECT timestamp FROM Carbon_Intensity_Data") #returns tuple of timestamps
    current = list(cur.fetchall())
    # print(current)
    existing_timestamps = []
    for tup in current:
        for t in tup: 
            existing_timestamps.append(t)

    #limit retrieval to 24 lines at a time
    for interval in r['data']['data']: 
        if count == 24: 
            break
        else: 
            #initializing variables that will go into database
            # print(interval)
            start_time = interval['from'][-6:-1] 
            # print(start_time)
            intensity_forecast = interval['intensity']['forecast']
            # print(intensity_forecast)
            date = interval['from'][8:10] + '-' + interval['from'][5:7] + '-' + interval['from'][:4]
            timestamp = interval['from'][-6:-1] + ' ' + interval['from'][8:10] + '-' + interval['from'][5:7] + '-' + interval['from'][:4]
            # print(timestamp)
            if timestamp in existing_timestamps: 
                continue
            else:
                cur.execute("INSERT OR IGNORE INTO Carbon_Intensity_Data (timestamp, date, time, intensity_forecast) VALUES (?,?,?,?)", (timestamp, date, start_time, intensity_forecast))
                count += 1 # count only increases IF not in database yet
    conn.commit()


def main():
    r = api_request('2024-04-09')
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + 'carbon_intensity.db')
    cur = conn.cursor()
    create_table(r, cur, conn)

if __name__ == "__main__":
    main()