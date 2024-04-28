import sqlite3
import requests
import json
import os
import visualizations
#import carbon intensity API

def api_request(startdate, enddate, regionid):
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
    from_ = f"{startdate}T00:30Z"
    to = f"{enddate}T24:00Z"
    r = requests.get(f'https://api.carbonintensity.org.uk/regional/intensity/{from_}/{to}/regionid/{regionid}', params={}, headers = headers).json()
    return r

def create_carbon_intensity_table(r, cur, conn):
    '''
    Arguments: takes the json output from the API request
    '''
    # cur.execute("DROP TABLE IF EXISTS Carbon_Intensity_Data")
    cur.execute(f"CREATE TABLE IF NOT EXISTS Carbon_Intensity_Data (Timestamp TEXT PRIMARY KEY, DNO_Region TEXT, Date TEXT, Time TEXT, Intensity_Forecast INTEGER)")

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
            dnoregion = r['data']['shortname']
            date = interval['from'][8:10] + '-' + interval['from'][5:7] + '-' + interval['from'][:4]
            timestamp = interval['from'][-6:-1] + ' ' + interval['from'][8:10] + '-' + interval['from'][5:7] + '-' + interval['from'][:4] + ' ' + dnoregion
            # print(timestamp)
            if timestamp in existing_timestamps: 
                continue
            else:
                cur.execute("INSERT OR IGNORE INTO Carbon_Intensity_Data (Timestamp, DNO_Region, Date, Time, Intensity_Forecast) VALUES (?,?,?,?,?)", (timestamp, dnoregion, date, start_time, intensity_forecast))
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
        for price in lst:
            accum += price
            count += 1
        avg = float(accum / count)
        avg_list.append((timestamp, round(avg, 2)))

    return avg_list

def create_generationmix_database(datadict, cur, conn):

    d = {}
    for interval in datadict['data']['data']:
        start_time = interval['from'][-6:-1] 
        date = interval['from'][8:10] + '-' + interval['from'][5:7] + '-' + interval['from'][:4]
        dnoregion = datadict['data']['shortname']
        timestamp = interval['from'][-6:-1] + ' ' + interval['from'][8:10] + '-' + interval['from'][5:7] + '-' + interval['from'][:4] + ' ' + dnoregion
        d[timestamp] = {'start_time': start_time, 'date': date, 'dnoregion': dnoregion}
        generationmix_dict = interval['generationmix']
        for mix in generationmix_dict:
            fuel = mix['fuel']
            percentage = mix['perc']
            d[timestamp][fuel] = percentage
    
    cur.execute('''CREATE TABLE IF NOT EXISTS Generation_Mix_Data
                (Timestamp TEXT PRIMARY KEY, 
                DNO_Region TEXT,
                Date TEXT, 
                Time TEXT, 
                Gas INTEGER, 
                Coal INTEGER, 
                Biomass INTEGER, 
                Nuclear INTEGER,
                Hydro INTEGER,
                Wind INTEGER,
                Solar INTEGER,
                Imports INTEGER,
                Other INTEGER)''')
    
    cur.execute("SELECT Timestamp FROM Generation_Mix_Data")
    existing = []
    retrieved = list(cur.fetchall())
    for keytup in retrieved:
        existing.append(keytup[0])

    # Insert data into database with the limit of 24 items or less
    loop_count = 0  
    for stamp, dct in d.items():
        cur.execute('''INSERT OR IGNORE INTO Generation_Mix_Data (Timestamp, DNO_Region, Date, Time, Gas, Coal, Biomass, Nuclear, Hydro, Wind, Solar, Imports, Other) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', (stamp, dct['dnoregion'], dct['date'], dct['start_time'], dct['gas'], dct['coal'], dct['biomass'], dct['nuclear'], dct['hydro'], dct['wind'], dct['solar'], dct['imports'], dct['other']))
        if stamp in existing:
            continue
        else:
            loop_count += 1

        if loop_count == 24:
            break

    conn.commit()
def calculate_average_generationmix(cur):
    cur.execute("SELECT Gas, Coal, Biomass, Nuclear, Hydro, Wind, Solar, Imports, Other FROM Generation_Mix_Data")
    lst = list(cur.fetchall())
    
    gas = 0
    coal = 0
    bio = 0
    nuke = 0
    hydro = 0
    wind = 0
    solar = 0
    imports = 0
    other = 0
    accum = 0
    for tup in lst:
        gas += tup[0]
        coal += tup[1]
        bio += tup[2]
        nuke += tup[3]
        hydro += tup[4]
        wind += tup[5]
        solar += tup[6]
        imports += tup[7]
        other += tup[8]
        accum += 1

    avg_mix = [('Gas', round(float(gas / accum), 2)), ('Coal', round(float(coal / accum), 2)), ('Biomass', round(float(bio / accum), 2)), ('Nuclear', round(float(nuke / accum),2)), ('Hydro', round(float(hydro / accum),2)), ('Wind', round(float(wind / accum),2)), ('Solar', round(float(solar / accum),2)), ('Imports', round(float(imports / accum),2)), ('Other', round(float(other / accum),2))]

    return avg_mix

def calculate_average_generationmix_timestamp(cur):
    '''
    NOT NECESSARY ANYMORE
    Calculates average percentage of generation mix 
    ARGUMENTS:
        Cursor: cur
    
    OUTPUT:
        Dict: {Timestamp:{Fuel: Average Percentage}}'''
    
    cur.execute("SELECT Time, Gas, Coal, Biomass, Nuclear, Hydro, Wind, Solar, Imports, Other FROM Generation_Mix_Data")
    d = {}
    avg = {}
    lst = list(cur.fetchall())
    
    for tup in lst:
        if tup[0] not in list(d.keys()):
            d[tup[0]] = {'Gas': tup[1], 'Coal': tup[2], 'Biomass': tup[3], 'Nuclear': tup[4], 'Hydro': tup[5], 'Wind': tup[6], 'Solar': tup[7], 'Imports': tup[8], 'Other': tup[9], 'count': 1}
        else:
            d[tup[0]]['Gas'] += tup[1]
            d[tup[0]]['Coal'] += tup[2]
            d[tup[0]]['Biomass'] += tup[3]
            d[tup[0]]['Nuclear'] += tup[4]
            d[tup[0]]['Hydro'] += tup[5]
            d[tup[0]]['Wind'] += tup[6]
            d[tup[0]]['Solar'] += tup[7]
            d[tup[0]]['Imports'] += tup[8]
            d[tup[0]]['Other'] += tup[9]
            d[tup[0]]['count'] += 1
    
    for timestamp, accumdict in d.items():
        avggas = float(accumdict['Gas'] / accumdict['count'])
        avgcoal = float(accumdict['Coal'] / accumdict['count'])
        avgbio = float(accumdict['Biomass'] / accumdict['count'])
        avgnuke = float(accumdict['Nuclear'] / accumdict['count'])
        avghydro = float(accumdict['Hydro'] / accumdict['count'])
        avgwind = float(accumdict['Wind'] / accumdict['count'])
        avgsolar = float(accumdict['Solar'] / accumdict['count'])
        avgimp = float(accumdict['Imports'] / accumdict['count'])
        avgother = float(accumdict['Other'] / accumdict['count'])
        avg[timestamp] = {'Gas': avggas, 'Coal': avgcoal, 'Biomass': avgbio, 'Nuclear': avgnuke, 'Hydro': avghydro, 'Wind': avgwind, 'Solar': avgsolar, 'Imports': avgimp, 'Other': avgother}

    return avg

def main():
    # cur, conn = visualizations.set_up_database('carbon_intensity.db')
    # intensity_api_dict = api_request('2024-04-21', '2024-04-27', 13)
    # for times in range(14):
    #         create_carbon_intensity_table(intensity_api_dict, cur, conn)
    # print(calculate_average_intensity_forecast(cur))
    pass
if __name__ == "__main__":
    main()  