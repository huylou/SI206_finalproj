import unittest
import sqlite3
import requests
import json
import os

#import carbon intensity API

def api_request(date, regionid):
    '''
    Arguments: Takes a date in string form in the format YYYY-MM-DD
    1	North Scotland
    2	South Scotland
    3	North West England
    4	North East England
    5	Yorkshire
    6	North Wales
    7	South Wales
    8	West Midlands
    9	East Midlands
    10	East England
    11	South West England
    12	South England
    13	London
    14	South East England
    15	England
    16	Scotland
    17	Wales

    Returns: The data from the website for that day in 30 minute intervals
    '''

    headers = {'Accept': 'application/json'}
    from_ = f"{date}T00:30Z"
    to = f"{date}T24:00Z"
    r = requests.get(f'https://api.carbonintensity.org.uk/regional/intensity/{from_}/{to}/regionid/{regionid}', params={}, headers = headers).json()
    return r

def create_table(r, cur, conn):
    '''
    Arguments: takes the json output from the API request
    '''

    # cur.execute("DROP TABLE IF EXISTS Carbon_Intensity_Data")
    cur.execute("CREATE TABLE IF NOT EXISTS Carbon_Intensity_Data (timestamp TEXT PRIMARY KEY, date TEXT, time TEXT, intensity_forecast INTEGER)")

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
            intensity_forecast = int(interval['intensity']['forecast'])
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

def calculate_average_intensity_forecast(cur):
    '''
    Calculates average intensity forecast per timestamp
    ARGUMENTS:
        Cursor: cur
    
    OUTPUT:
        List of Tuples (Timestamp, Average Intensity Forecast Value)'''
    
    cur.execute("SELECT time, intensity_forecast FROM Carbon_Intensity_Data")
    d = {}
    lst = list(cur.fetchall())
    avg_list = []
    
    for tup in lst:
        if tup[0] not in list(d.keys()):
            d[tup[0]] = []
        d[tup[0]].append(tup[1])
    
    for timestamp, lst in d.items():
        accum = 0
        count = 0
        for inten in lst:
            accum += inten
            count += 1
        avg = float(accum / count)
        avg_list.append((timestamp, round(avg, 2)))
    
    return avg_list


def main():
    r = api_request('2024-04-09', 13)
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + 'carbon_intensity.db')
    cur = conn.cursor()
    create_table(r, cur, conn)
    avg_inten_list = calculate_average_intensity_forecast(cur)
    
if __name__ == "__main__":
    main()