import unittest
import sqlite3
import requests
import json
import os
import re


def set_up_database(db_name):
    """
    Sets up a SQLite database connection and cursor.

    ARGUMENTS:
        db_name: str
            The name of the SQLite database.

    OUTPUT:
        Tuple (Cursor, Connection):
            A tuple containing the database cursor and connection.
    """
    filepath = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(filepath + "/" + db_name)
    cur = conn.cursor()
    return cur, conn

def get_electricity_costs_dict(dnonum, voltagelevel, start, end):
    '''
    Call UK Electricity Costs API, returns json dictionary containing timestamp and price/kWh data based on a half-hourly (HH) interval

    ARGUMENTS: 
        DNO Region Numbers (int):
            10 – Eastern England
            11 – East Midlands
            12 – London
            13 – North Wales
            14 – West Midlands
            15 – North East
            16 – North West
            17 – Northern Scotland
            18 – Southern Scotland
            19 – South East
            20 – Southern
            21 – South Wales
            22 – South Western
            23 – Yorkshire

        Voltage Levels (str):
            HV - High Voltage
            LV - Low Voltage
            LV-Sub - Low Voltage - Sub

        start, end date format(str): DD-MM-YYYY
        
    OUTPUT: json dictionary (dict)
    '''
    dno = f'dno={dnonum}'
    voltage = f'voltage={voltagelevel}'
    startdate = f'start={start}'
    enddate = f'end={end}'
    r = requests.get(f'https://odegdcpnma.execute-api.eu-west-2.amazonaws.com/development/prices?{dno}&{voltage}&{startdate}&{enddate}').json()
    return r

def retrieve_price_and_timestamp(jsondict, dnodict):
    '''
    Retrieve overall price per kWh and timestamp (XX:XX DD-MM-YYYY)

    ARGUMENTS:
        jsondict (dict): Json dictionary of electricity costs API
    
    OUTPUT:
        List:
            List of tuples (Price, Time, Date, Timestamp) containing the overall price of the timestamp and date (DD-MM-YYYY)
            
    '''
    lst = []
    dnoregion = dnodict[int(jsondict['data']['dnoRegion'])]
    for dct in jsondict['data']['data']:
        price = float(dct['Overall'])
        timestamp = f"{dct['Timestamp']} {dnoregion}"
        date = re.findall(r'\d{2}\-\d{2}\-\d{4}', timestamp)[0]
        time = re.findall(r'\d{2}\:\d{2}', timestamp)[0]
        tup = (price, time, date, dnoregion, timestamp)
        lst.append(tup)
    return lst

def create_electricitycost_table_with_limit(datalist, cur, conn):
    '''
    Uses functions to pull data from electricity costs API and organize into data structure
    to create a database storing electricity costs data, with a data injection limit of 24

    ARGUMENTS:
        Datalist: List of tuples (Price, Time, Date) from Electricity Costs API
        Cursor: cur
        Connection: conn
    
    OUTPUT:
        None, Database table created or updated
            
    '''
    # Create table in database if it does not exist 
    cur.execute(f'''CREATE TABLE IF NOT EXISTS Electricity_Costs
                (Timestamp TEXT PRIMARY KEY,
                DNO_Region TEXT,
                Date TEXT, 
                Time TEXT,
                Price_per_KWh INTEGER)''')
    
    # Create list of existing keys in database
    cur.execute("SELECT Timestamp, DNO_Region FROM Electricity_Costs")
    existing = []
    retrieved = list(cur.fetchall())
    for keytup in retrieved:
        existing.append(keytup[0])

    # Insert data into database with the limit of 24 items or less
    loop_count = 0  
    for tup in datalist:
        key = tup[4]
        cur.execute(f"INSERT OR IGNORE INTO Electricity_Costs (Timestamp, DNO_Region, Date, Time, Price_per_KWh) VALUES (?,?,?,?,?)", (tup[4], tup[3], tup[2], tup[1], tup[0]))
        if key in existing:
            continue
        else:
            loop_count += 1

        if loop_count == 24:
            break

    conn.commit()

def calculate_average_electricity_costs(cur):
    '''
    Calculates average electricity costs per timestamp
    ARGUMENTS:
        Cursor: cur
    
    OUTPUT:
        List of Tuples (Timestamp, Average Price per kWh)'''
    
    cur.execute("SELECT Time, Price_per_KWh FROM Electricity_Costs")
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
        for price in lst:
            accum += price
            count += 1
        avg = float(accum / count)
        avg_list.append((timestamp, round(avg, 2)))

    return avg_list

def main():
    dnoregiondict = {10:'East England', 11:'East Midlands',  12:'London',  13:'North Wales', 14:'West Midlands', 15:'North East England', 16:'North West England', 17:'North Scotland', 18:'South Scotland', 19:'South East England', 20: 'South England', 21: 'South Wales', 22: 'South West England', 23: 'Yorkshire'}
    cur, conn = set_up_database("electricity_costs.db")
    for dnonum in dnoregiondict.keys():
        electricity_costs_dict = get_electricity_costs_dict(dnonum, 'HV', '21-04-2024', '27-04-2024')
        price_list = retrieve_price_and_timestamp(electricity_costs_dict, dnoregiondict)
        for times in range(14):
            create_electricitycost_table_with_limit(price_list, cur, conn)
    conn.close()

if __name__ == "__main__":
    main()
    
