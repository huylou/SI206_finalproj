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
            13 – North Wales & Mersey
            14 – Midlands
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

def retrieve_price_and_timestamp(jsondict):
    '''
    Retrieve overall price per kWh and timestamp (XX:XX DD-MM-YYYY)

    ARGUMENTS:
        jsondict (dict): Json dictionary of electricity costs API
    
    OUTPUT:
        List:
            List of tuples (Price, Time, Date) containing the overall price of the timestamp and date (DD-MM-YYYY)
            
    '''
    lst = []
    for dct in jsondict['data']['data']:
        price = float(dct['Overall'])
        timestamp = dct['Timestamp']
        date = re.findall(r'\d{2}\-\d{2}\-\d{4}', timestamp)[0]
        time = re.findall(r'\d{2}\:\d{2}', timestamp)[0]
        tup = (price, time, date, timestamp)
        lst.append(tup)
    return lst

def create_electricitycost_table(dnonum, voltagelevel, start, end, cur, conn):
    electricity_costs_dict = get_electricity_costs_dict(dnonum, voltagelevel, start, end)
    price_list = retrieve_price_and_timestamp(electricity_costs_dict)
    naming = start.split("-")
    naming2 = end.split("-")
    title = f"Electricity_Costs_{naming[0]}_{naming[1]}_{naming[2]}_to_{naming2[0]}_{naming2[1]}_{naming2[2]}"
    cur.execute(f"DROP TABLE IF EXISTS {title}")
    cur.execute(f'''CREATE TABLE IF NOT EXISTS {title}
                (Timestamp TEXT PRIMARY KEY, 
                Time TEXT,
                Price_per_KWh INTEGER)''')
    for tup in price_list:
         cur.execute(f"INSERT OR IGNORE INTO Electricity_Costs_{naming[0]}_{naming[1]}_{naming[2]}_to_{naming2[0]}_{naming2[1]}_{naming2[2]} (Timestamp, Time, Price_per_KWh) VALUES (?,?,?)", (tup[3], tup[1], tup[0]))
    conn.commit()

def main():
    cur, conn = set_up_database("electricity_costs.db")
    electricity_costs_dict = get_electricity_costs_dict(12, 'HV', '09-04-2024', '10-04-2024')
    create_electricitycost_table(12, 'HV', '09-04-2024', '10-04-2024', cur, conn)
    conn.close()

if __name__ == "__main__":
    main()
    
