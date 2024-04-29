import matplotlib.pyplot as plt
import csv
import os
import sqlite3
import pandas as pd
import geopandas as gpd #install geopandas: conda install geopandas
import mapclassify #conda install -c conda-forge mapclassify
import electricity_costs
import carbon_intensity

''' This .py file performs calculations and visualizations of our database data
    INSTALL MODULES THRU ANACONDA PROMPT'''

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

def intensity_avg_csv_writer(cur, dnodict):

    """

    Write data to a CSV file.

    Args:

        data (dict): A dictionary containing data to be written to the CSV file.

        filename (str): 
    """
    with open('avg_intensity_dnoregions.csv', 'w', encoding='utf-8-sig') as file:
        csv_writer = csv.writer(file)
        header = ['ID', 'dno_region', 'average_carbon_intensity']
        csv_writer.writerow(header)

        for dno_id, dnoregion in dnodict.items():
            tup = carbon_intensity.calculate_average_carbonintensity_region(cur, dnoregion)
            csv_writer.writerow([dno_id, tup[0], tup[1]])

def avg_cost_intensity_calculation_region(cur, dnoregion):
    cur.execute(f'''
                SELECT Carbon_Intensity_Data.Time, Carbon_Intensity_Data.Date, Carbon_Intensity_Data.Intensity_Forecast, Electricity_Costs.Price_per_KWh  
                FROM Carbon_Intensity_Data
                INNER JOIN Electricity_Costs ON Carbon_Intensity_Data.Timestamp = Electricity_Costs.Timestamp
                WHERE Electricity_Costs.DNO_Region = '{dnoregion}'
                ''')
    lst = cur.fetchall()
    d = {}
    avg_dict = {}
    for tup in lst:
        if tup[0] not in list(d.keys()):
            d[tup[0]] = {'intensity_forecast': tup[2], 'price': tup[3], 'count': 1}
        else:
            d[tup[0]]['intensity_forecast'] += tup[2]
            d[tup[0]]['price'] += tup[3]
            d[tup[0]]['count'] += 1

    for timestamp, accumdict in d.items():
        avgintensity = round(float(accumdict['intensity_forecast'] / accumdict['count']),2)
        avgprice = round(float(accumdict['price'] / accumdict['count']),2)
        avg_dict[timestamp] = {'average_intensity': avgintensity, 'average_price': avgprice}
    
    return avg_dict

def avg_cost_to_intensity_linechart_dnoregion(cur, dnoregion):
    avg_dict = avg_cost_intensity_calculation_region(cur, dnoregion)
    time_axis = []
    y_intensity= []
    y_price_per_kwh = []
    for time, dct in avg_dict.items():
        time_axis.append(time)
        y_intensity.append(dct['average_intensity'])
        y_price_per_kwh.append(dct['average_price'])
  
    fig, ax1 = plt.subplots()
    ax1_color = 'tab:red'
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Carbon Intensity (gCO2/kWh)', color=ax1_color)
    ax1.plot(time_axis, y_intensity, color=ax1_color)
    plt.xticks(rotation=60)

    ax2 = ax1.twinx()
    ax2_color = 'tab:blue'
    ax2.set_ylabel('£/kWh', color=ax2_color)  
    ax2.plot(time_axis, y_price_per_kwh, color=ax2_color)
    plt.xticks(rotation=60)

    plt.rc('xtick', labelsize=8) 
    fig.tight_layout() 
    
    plt.title(f"Average Carbon Intensity vs. Average Electricity Costs in {dnoregion} Over Time")
    plt.show()

def avg_cost_to_intensity_linechart_alldnoregion(cur, dnodict):
    fig = plt.figure(1, figsize=[15, 10])
    font = {'family' : 'arial',
        'weight' : 'normal',
        'size'   : 4}
    
    plt.rc('font', **font)
    plt.rc('xtick', labelsize=3) 
    pos = 1
    for dnoregion in dnodict.values():
        avg_dict = avg_cost_intensity_calculation_region(cur, dnoregion)
        time_axis = []
        y_intensity= []
        y_price_per_kwh = []
        for time, dct in avg_dict.items():
            time_axis.append(time)
            y_intensity.append(dct['average_intensity'])
            y_price_per_kwh.append(dct['average_price'])
        ax1 = fig.add_subplot(4,4,pos)
        ax1_color = 'tab:red'
        ax1.set_xlabel('Time', loc='left')
        ax1.set_ylabel('Carbon Intensity (gCO2/kWh)', color=ax1_color, fontsize = 6)
        ax1.plot(time_axis, y_intensity, color=ax1_color)
        plt.xticks(rotation=60)

        ax2 = ax1.twinx()
        ax2_color = 'tab:blue'
        ax2.set_ylabel('£/kWh', color=ax2_color, fontsize = 6)  
        ax2.plot(time_axis, y_price_per_kwh, color=ax2_color) 
        
        plt.title(f"Average Carbon Intensity vs. Average Electricity Costs in {dnoregion} Over Time", fontsize = 5)
        pos += 1
    fig.tight_layout(pad=0.5)
    plt.show()

def generation_mix_piechart(cur):
    lst = carbon_intensity.calculate_average_generationmix(cur)
    labels = []
    values = []
    for tup in lst:
        labels.append(tup[0])
        values.append(tup[1])

    fig, ax = plt.subplots()
    ax.pie(values, labels=labels, autopct='%1.1f%%',textprops={'fontsize': 12})
    plt.title("UK Power Generation Mix by Source", fontsize = 14)
    plt.show()

def avg_intensity_uk_dnoregion_geospatial(dno_csv, geojson):
    gdf = gpd.read_file(geojson)
    dno_df = pd.read_csv(dno_csv)
    gdf = gdf.merge(dno_df, on='ID')
    gdf.plot(column='average_carbon_intensity', figsize=(20, 10), scheme="Quantiles", k=14, linewidth=0.1, edgecolor="grey", legend=True, legend_kwds={'title': '(gCO2/kWh)', 'title_fontsize': 5, 'fontsize': 5, 'markerscale': 0.6}, cmap='YlOrRd').set_axis_off()
    plt.title("Average Carbon Intensity by UK DNO Regions", fontsize = 14)
    plt.show()

def main():
    #Database set up
    cur, conn = set_up_database('uk_electricitycosts_carbonintensity_data.db')
    dnoregiondict = {10:'East England', 
                     11:'East Midlands',  
                     12:'London',  
                     13:'North Wales & Merseyside', 
                     14:'West Midlands', 
                     15:'North East England', 
                     16:'North West England', 
                     17:'North Scotland', 
                     18:'South Scotland', 
                     19:'South East England', 
                     20:'South England', 
                     21:'South Wales', 
                     22:'South West England', 
                     23:'Yorkshire'}
    # Create Database Table for Electricity Costs
    for dnonum in dnoregiondict.keys():
        electricity_costs_dict = electricity_costs.get_electricity_costs_dict(dnonum, 'HV', '21-04-2024', '28-04-2024')
        price_list = electricity_costs.retrieve_price_and_timestamp(electricity_costs_dict, dnoregiondict)
        for times in range(14):
            electricity_costs.create_electricitycost_table_with_limit(price_list, cur, conn)
    
     # Create Database Table for Carbon Intensity and Generation Mix
    for regionnum in range(1,15):
        intensity_api_dict = carbon_intensity.api_request('2024-04-21', '2024-04-27', regionnum)
        for times in range(14):
            carbon_intensity.create_carbon_intensity_table(intensity_api_dict, cur, conn)
            carbon_intensity.create_generationmix_database(intensity_api_dict, cur, conn) 

    #Calculations to file
    intensity_avg_csv_writer(cur, dnoregiondict)
    #!!! write more functions to collect data into file

    # Data Visualization
    avg_cost_to_intensity_linechart_dnoregion(cur, 'London') #linechart individual
    avg_cost_to_intensity_linechart_alldnoregion(cur, dnoregiondict) #linechart all regions
    generation_mix_piechart(cur) #piechart
    avg_intensity_uk_dnoregion_geospatial('avg_intensity_dnoregions.csv', 'DNO_License_Areas_20200506.geojson') #geospatial viz
    conn.close()

if __name__ == "__main__":
    main()


