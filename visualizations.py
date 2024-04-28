import matplotlib.pyplot as plt
import os
import sqlite3
import electricity_costs
import carbon_intensity

''' This .py file performs calculations and visualizations of our database data'''

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

def avg_cost_intensity_calculation_region(cur, dnoregion):
    cur.execute(f'''
                SELECT Electricity_Costs.Time, Electricity_Costs.Price_per_KWh, Carbon_Intensity_Data.Intensity_Forecast 
                FROM Electricity_Costs
                JOIN Carbon_Intensity_Data ON Electricity_Costs.DNO_Region = Carbon_Intensity_Data.DNO_Region
                WHERE Carbon_Intensity_Data.DNO_Region = '{dnoregion}'
                ''')
    lst = cur.fetchall()
    d = {}
    avg_dict = {}
    for tup in lst:
        if tup[0] not in list(d.keys()):
            d[tup[0]] = {'intensity_forecast': tup[2], 'price': tup[1], 'count': 1}
        else:
            d[tup[0]]['intensity_forecast'] += tup[2]
            d[tup[0]]['price'] += tup[1]
            d[tup[0]]['count'] += 1

    for timestamp, accumdict in d.items():
        avgintensity = round(float(accumdict['intensity_forecast'] / accumdict['count']),2)
        avgprice = round(float(accumdict['price'] / accumdict['count']),2)
        avg_dict[timestamp] = {'average_intensity': avgintensity, 'average_price': avgprice}
    
    return avg_dict

def avg_cost_to_intensity_linechart_dnoregion(cur, dnoregion):
    avg_dict = avg_cost_intensity_calculation_region(cur, dnoregion)
    pass
def generation_mix_piechart():
    pass

def uk_dnoregion_geospatial():
    pass

def main():
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
    
    # for dnonum in dnoregiondict.keys():
    #     electricity_costs_dict = electricity_costs.get_electricity_costs_dict(dnonum, 'HV', '21-04-2024', '28-04-2024')
    #     price_list = electricity_costs.retrieve_price_and_timestamp(electricity_costs_dict, dnoregiondict)
    #     for times in range(14):
    #         electricity_costs.create_electricitycost_table_with_limit(price_list, cur, conn)
    
    # for regionnum in range(1,15):
    #     intensity_api_dict = carbon_intensity.api_request('2024-04-21', '2024-04-27', regionnum)
    #     for times in range(14):
    #         carbon_intensity.create_carbon_intensity_table(intensity_api_dict, cur, conn)
    #         carbon_intensity.create_generationmix_database(intensity_api_dict, cur, conn) 
    print(avg_cost_intensity_calculation_region(cur, 'London'))
    conn.close()

if __name__ == "__main__":
    main()


