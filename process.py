import pandas as pd
import numpy as np
from datetime import datetime
import os

# function to determine Metro/ROI
def ctype(city_name):
    if city_name in city:
        return 'Metro'
    else:
        return 'ROI'
        
        
os.chdir('/home/siddharth/uber_auto/input')

files = os.listdir()

# get city list
cities = pd.read_excel('/home/siddharth/uber_auto/CityClassification.xlsx', header = 1, engine='openpyxl')
cities.drop(columns='Unnamed: 0', inplace = True)
city = {}


for i in range(len(cities)):
    if cities.iloc[i]['Type of City'] == 'Metro':
        city[cities.iloc[i]['City']] = cities.iloc[i]['Type of City']
        



for file in files:
    sample = pd.read_csv(file)
    sample.insert(loc=0, column='Type of City', value='ROI')
    sample['Type of City'] = sample['src_city'].apply(ctype)
    sample.insert(loc=1, column='Date', value=sample['date'])
    sample.drop('date', axis=1, inplace=True)
    
    target_metro = pd.read_csv('/home/siddharth/uber_auto/output/data_metro.csv')
    target_roi = pd.read_csv('/home/siddharth/uber_auto/output/data_roi.csv')
    roi_sample = sample[sample['Type of City'] == 'ROI']
    metro_sample = sample[sample['Type of City'] == 'Metro']
    del sample
    
    metro_sample.drop('Type of City', axis = 1, inplace=True)
    roi_sample.drop('Type of City', axis = 1, inplace=True)
    
    metro_sample['category'] = metro_sample.apply(lambda x: f"{str(x['platform'])}_{str(x['category_name'])}", axis=1)
    
    roi_sample['category'] = roi_sample.apply(lambda x: f"{str(x['platform'])}_{str(x['category_name'])}", axis=1)
    
    categories_main = np.array(['ola_auto', 'ola_bike', 'ola_micro', 'ola_mini', 'ola_prime_sedan',
                        'ola_prime_play', 'ola_prime_suv', 'uber_auto', 'uber_moto', 'uber_ubergo',
                        'uber_premier', 'uber_uberxl', 'rapido_bike', 'rapido_auto'])
    
    cats = {}
    for i in categories_main:
        cats[i] = 1
        
    metro_sample['use'] = metro_sample.apply(lambda x: x['category'] in cats, axis=1)
    roi_sample['use'] = roi_sample.apply(lambda x: x['category'] in cats, axis=1)
    
    metro_sample_use = metro_sample[metro_sample['use']==True]
    roi_sample_use = roi_sample[roi_sample['use']==True]
    
    del metro_sample, roi_sample
    
    metro_pre = pd.DataFrame(columns=np.arange(87))
    metro_pre.columns = target_metro.columns
    
    roi_pre = pd.DataFrame(columns=np.arange(87))
    roi_pre.columns = target_roi.columns
    
    pairs = np.unique(np.array(metro_sample_use.set_index(['src_place_id', 'dest_place_id', 'timestamp_iso']).index))
    keylist = list(target_metro.columns)
    defdict = {key: None for key in keylist}
    
    rows = []
    # processing metro data first
    for i in pairs:
        df = metro_sample_use[(metro_sample_use['src_place_id'] == i[0])
                              & (metro_sample_use['dest_place_id'] == i[1]) &
                              (metro_sample_use['timestamp_iso'] == i[2])]
        row_current = defdict.copy()

        row_current['distance_in_km'] = df['distance_in_km'].values[0]
        row_current['Date'] = df['Date'].values[0]
        row_current['source_city'] = df['src_city'].values[0]
        row_current['source_place_id'] = df['src_place_id'].values[0]
        row_current['destination_place_id']= df['dest_place_id'].values[0]
        row_current['group_time'] = df['time'].values[0]
        row_current['source_country'] = df['country'].values[0]
        for j in range(len(df)):
            if df.iloc[j]['category'] == 'uber_ubergo':
                row_current[keylist[56]] = df.iloc[j]['num_available']
                row_current[keylist[57]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[58]] = df.iloc[j]['upfront_fare']
                row_current[keylist[59]] = df.iloc[j]['is_surging']
                row_current[keylist[60]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'uber_uberxl':
                row_current[keylist[66]] = df.iloc[j]['num_available']
                row_current[keylist[67]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[68]] = df.iloc[j]['upfront_fare']
                row_current[keylist[69]] = df.iloc[j]['is_surging']
                row_current[keylist[70]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'uber_premier':
                row_current[keylist[61]] = df.iloc[j]['num_available']
                row_current[keylist[62]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[63]] = df.iloc[j]['upfront_fare']
                row_current[keylist[64]] = df.iloc[j]['is_surging']
                row_current[keylist[65]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'uber_auto':
                row_current[keylist[46]] = df.iloc[j]['num_available']
                row_current[keylist[47]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[48]] = df.iloc[j]['upfront_fare']
                row_current[keylist[49]] = df.iloc[j]['is_surging']
                row_current[keylist[50]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'uber_moto':
                row_current[keylist[51]] = df.iloc[j]['num_available']
                row_current[keylist[52]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[53]] = df.iloc[j]['upfront_fare']
                row_current[keylist[54]] = df.iloc[j]['is_surging']
                row_current[keylist[55]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'rapido_bike':
                row_current[keylist[71]] = df.iloc[j]['num_available']
                row_current[keylist[72]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[73]] = df.iloc[j]['upfront_fare']
                row_current[keylist[74]] = df.iloc[j]['discounted_fare']
                row_current[keylist[75]] = df.iloc[j]['is_surging']
                row_current[keylist[76]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'rapido_auto':
                row_current[keylist[77]] = df.iloc[j]['num_available']
                row_current[keylist[78]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[79]] = df.iloc[j]['upfront_fare']
                row_current[keylist[80]] = df.iloc[j]['discounted_fare']
                row_current[keylist[81]] = df.iloc[j]['is_surging']
                row_current[keylist[82]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'ola_auto':
                row_current[keylist[11]] = df.iloc[j]['num_available']
                row_current[keylist[12]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[13]] = df.iloc[j]['upfront_fare']
                row_current[keylist[14]] = df.iloc[j]['is_surging']
                row_current[keylist[15]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'ola_bike':
                row_current[keylist[16]] = df.iloc[j]['num_available']
                row_current[keylist[17]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[18]] = df.iloc[j]['upfront_fare']
                row_current[keylist[19]] = df.iloc[j]['is_surging']
                row_current[keylist[20]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'ola_micro':
                row_current[keylist[21]] = df.iloc[j]['num_available']
                row_current[keylist[22]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[23]] = df.iloc[j]['upfront_fare']
                row_current[keylist[24]] = df.iloc[j]['is_surging']
                row_current[keylist[25]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'ola_mini':
                row_current[keylist[26]] = df.iloc[j]['num_available']
                row_current[keylist[27]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[28]] = df.iloc[j]['upfront_fare']
                row_current[keylist[29]] = df.iloc[j]['is_surging']
                row_current[keylist[30]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'ola_prime_sedan':
                row_current[keylist[31]] = df.iloc[j]['num_available']
                row_current[keylist[32]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[33]] = df.iloc[j]['upfront_fare']
                row_current[keylist[34]] = df.iloc[j]['is_surging']
                row_current[keylist[35]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'ola_prime_play':
                row_current[keylist[36]] = df.iloc[j]['num_available']
                row_current[keylist[37]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[38]] = df.iloc[j]['upfront_fare']
                row_current[keylist[39]] = df.iloc[j]['is_surging']
                row_current[keylist[40]] = df.iloc[j]['surge_multipler']
            elif df.iloc[j]['category'] == 'ola_prime_suv':
                row_current[keylist[41]] = df.iloc[j]['num_available']
                row_current[keylist[42]] = df.iloc[j]['wait_time_in_min']
                row_current[keylist[43]] = df.iloc[j]['upfront_fare']
                row_current[keylist[44]] = df.iloc[j]['is_surging']
                row_current[keylist[45]] = df.iloc[j]['surge_multipler']

        rows.append(row_current)

    metro_pre = pd.DataFrame(rows)
    metro_pre['source_country'] = 'India'
    s_n = target_metro['S. No.'].values[-1]
    num_list = []
    for i in range(len(metro_pre)):
        num_list.append(i + s_n + 1)
    
    metro_pre['S. No.'] = num_list

    # truncating at the top to keep under 1 million rows
    day_1_metro = target_metro.iloc[0]['Date']
    target_metro = target_metro[target_metro['Date'] > day_1_metro]
    metro_pre.replace(True, 'Y', inplace=True)
    metro_pre.replace(False, 'N', inplace=True)
    
    ## read and add formulae here 
    # read from text file. in order of, Uber_auto, Uber_go, Uber_premium, Uber_moto
    
    f1_list = []
    f2_list = []
    f3_list = []
    f4_list = []
    for i in range(1032572, 1032572 + len(metro_pre)):
        f1 = f'=IF(BG{i}<>0,IF(D{i}="Chennai",BG{i}-MIN(BG{i}*5%,50),IF(D{i}="Bangalore",BG{i}-MIN(BG{i}*6%,50),IF(D{i}="Mumbai",BG{i}-MIN(BG{i}*1%,50),IF(D{i}="Hyderabad",BG{i}-MIN(BG{i}*4%,40),BG{i})))),BG{i})'
        f2 = f'=IF(BL{i}<>0,IF(D{i}="Chennai",BL{i}-MIN(BL{i}*5%,50),IF(D{i}="Bangalore",BL{i}-MIN(BL{i}*6%,50),IF(D{i}="Mumbai",BL{i}-MIN(BL{i}*1%,50),IF(D{i}="Hyderabad",BL{i}-MIN(BL{i}*4%,40),BL{i})))),BL{i})'
        f3 = f'=IF(BB{i}<>0,IF(D{i}="Hyderabad",BB{i}-MIN(BB{i}*7%,10),IF(D{i}="Chennai",BB{i}-MIN(BB{i}*7%,10),IF(D{i}="Delhi NCR",BB{i}-MIN(BB{i}*7%,10),BB{i}))),BB{i})'
        f4 = f'=IF(AND(AW{i}<>0,D{i}<>"Kolkata"),IF(D{i}="Bangalore",(AW{i}-25),IF(D{i}="Delhi NCR",(AW{i}-25),IF(D{i}="Mumbai",(AW{i}-25),IF(D{i}="Hyderabad",(AW{i}-25),IF(D{i}="Chennai",(AW{i}-25),IF(D{i}="Pune",(AW{i}-25),IF(D{i}="Ahmedabad",(AW{i}-29)))))))),"")'
        f1_list.append(f1)
        f2_list.append(f2)
        f3_list.append(f3)
        f4_list.append(f4)

    metro_pre['Uber_Go_discounted_fare'] = f2_list
    metro_pre['Uber_Premier_discounted_fare'] = f3_list
    metro_pre['Uber_moto_discounted_fare'] = f4_list
    metro_pre['Uber_auto_discounted_fare'] = f1_list

    output_metro = pd.concat([target_metro, metro_pre])

    # save metro output
    output_metro.to_csv('/home/siddharth/uber_auto/output/data_metro.csv', index=False)

    # process roi data


# code to upload to cloud

