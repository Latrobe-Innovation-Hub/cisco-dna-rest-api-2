# written by: Andrew McDonald
# current: 17/07/23
# version: 0.3

import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import zlib
from io import StringIO
from datetime import datetime, timedelta
from tqdm import tqdm
from num2words import num2words
from tabulate import tabulate
import time


## ===============================================================
## request header
## ===============================================================

headers = CaseInsensitiveDict()
headers["Accept"] = "*/*"
headers["Accept-Encoding"] = "gzip, deflate, br"
headers["Connection"] = "keep-alive"
headers["Content-Type"] = "application/json"
headers["Authorization"] = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYnkiOiJMb2NhdGlvbiIsInR5cGUiOiJCZWFyZXIiLCJ0ZW5hbnRJZCI6MTY3NjgsInVzZXJuYW1lIjoiYW5kcmV3Lm1jZG9uYWxkQGxhdHJvYmUuZWR1LmF1Iiwia2V5SWQiOiJmOGJhZTEyMi1hNTgzLTRjMmYtYjEwYS0yMTQ2M2NjNjY0ZDIiLCJ1c2VySWQiOjM5ODk2LCJpYXQiOjE2ODE3MDQ5NjQsImV4cCI6MTcxMzI0MDk2M30.c4OZHpP8-dsvPmhXiJNuLLLzGVr4JjZ2wm0AtqkNReA"


## ===============================================================
## time interval creator function
## ===============================================================

def create_time_interval(intervals, interval_length):
    # if needed, can set init date to 'yesterday' to ensure data exists (error?)
    # so far testing with time 'now' is working...
    now = datetime.now() #- timedelta(days = 1)

    #set hours= for hours
    #set dayys= for days
    #set minutes=  for minutes
    interval_list = [now - timedelta(hours=int(interval_length)*x) for x in range(0, int(intervals)+1)]

    return interval_list

from datetime import datetime, timedelta

def create_date_interval(start_date, intervals):
    # Convert the start_date to a datetime object
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')

    # Create the interval list
    interval_list = [start_datetime + timedelta(days=x) for x in range(int(intervals))]

    return interval_list

# to use the above date interval function:
# intervals = create_date_interval('2023-07-01', 5)
# for interval in intervals:
#    print(interval)

## ===============================================================
## build campus ID dictionary function
## ===============================================================

from tqdm import tqdm

def create_campus_dictionary():
    url = "https://dnaspaces.io/api/location/v1/map/hierarchy"
    response = requests.get(url, headers=headers)

    campus_list = response.json()['map']
    campus_dict = {}

    for x in tqdm(range(len(campus_list))):
        #print(campus_list[x]['name'], campus_list[x]['id'])
        campus_dict[campus_list[x]['name']] = campus_list[x]['id']

    return campus_dict


## ===============================================================
## build building ID dictionary function
## ===============================================================
def create_building_dictionary(campus_dict, campus):
    url = f"https://dnaspaces.io/api/location/v1/map/elements/{campus_dict[campus]}"
    response = requests.get(url, headers=headers)
    
    buildings_list = response.json()['map']['relationshipData']['children']
    buildings_dict = {building['name']: building['id'] for building in buildings_list}
    
    return buildings_dict


## ===============================================================
## build floor dictionary function
## ===============================================================
def create_floor_dictionary(campus_name):
    url = f"https://dnaspaces.io/api/location/v1/map/hierarchy/"
    response = requests.get(url, headers=headers)
    
    # Use dictionary comprehension to filter out only elements with matching campus_name
    campus_map = next((item for item in response.json()['map'] if item['name'] == campus_name), None)
    if campus_map is None:
        return None, None

    building_dict = {}
    for building in campus_map['relationshipData']['children']:
        building_dict[building['name']] = building['id']

    floors_dict = {}
    for b in tqdm(range(len(campus_map['relationshipData']['children'])), leave=True):
        building_map = campus_map['relationshipData']['children'][b]
        building_name = building_map['name']

        floors_dict[building_name] = {}
        for c in range(len(building_map['relationshipData']['children'])):
            floor_map = building_map['relationshipData']['children'][c]
            floors_dict[building_name][f"Floor {c+1}"] = {
                'id': floor_map['id'],
                'img': floor_map['details']['image']['imageName'],
                'x': floor_map['details']['width'],
                'y': floor_map['details']['length'],
                'img_width': floor_map['details']['image']['width'],
                'img_height': floor_map['details']['image']['height'],
                'height': floor_map['details']['height'],
                'gps_markers': floor_map['details']['gpsMarkers'],
                'calibration_model': floor_map['details']['calibrationModel']
            }

    return building_dict, floors_dict
    
## ===============================================================
## Get building floor data function
## ===============================================================

def get_data(building_id, floor_id, intervals, interval_list):
    url = f"https://dnaspaces.io/api/location/v1/history/records/"

    # example time range - this should be true!
    # 1669510800000 = Sun Nov 27 2022 12:00:00 GMT+1100 (Australian Eastern Daylight Time)
    # 1669514400000 = Sun Nov 27 2022 13:00:00 GMT+1100 (Australian Eastern Daylight Time)

    # disable chained assignments
    pd.options.mode.chained_assignment = None
    
    data_all = None
    df1 = pd.DataFrame()
    all_data = pd.DataFrame()

    for x in tqdm(range(int(intervals))):
        query = {'buildingId': f'{building_id}',
                 'floorId': f'{floor_id}',
                 #'deviceType':'CLIENT',
                 'startTime':int(interval_list[x+1].timestamp() * 1000),
                 'endTime':int(interval_list[x].timestamp() * 1000),
        }

        response = requests.get(url, headers=headers, params=query)

        decompressed_data = zlib.decompress(response.content, zlib.MAX_WBITS|16)

        # Assign column names
        columns=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','aa','ab','ac','ad','ae','af','ag']

        data_all = str(decompressed_data,'utf-8')
        decompressed_data = None

        data = StringIO(data_all) 
        df1 = pd.read_csv(data, names=columns)

        # concatenate with previous data
        all_data = pd.concat([all_data, df1])

    return all_data


## ===============================================================
## Save data to csv function
## ===============================================================
def save_to_csv(data, filename,  sort=True):
    # Check if filename ends with '.csv', and add it if it doesn't
    if not filename.endswith('.csv'):
        filename = filename + '.csv'
        
    if sort:
        # sort by column 'j' timestamp
        data = data.sort_values(by='j')

    # Create a Pandas dataframe from the data
    df = pd.DataFrame(data)

    # Write the dataframe to a csv file
    df.to_csv(filename, index=False)
    
    return df


def main():
    # build locations dictionary
    campus_dict = create_campus_dictionary()

    # get user choice for location
    print('\n === Locations ===')
    num_columns = 3
    locations = list(campus_dict.keys())
    num_locations = len(locations)
    num_rows = (num_locations + num_columns - 1) // num_columns

    for row in range(num_rows):
        for col in range(num_columns):
            index = col*num_rows + row
            if index < num_locations:
                loc_str = f"{index+1}: {locations[index]}"
                padded_str = loc_str.ljust(30)
                print(padded_str, end="")
        print()
    selected_index = input('\nEnter the number of the location you want to select: ')
    selected_location = list(campus_dict.keys())[int(selected_index)-1]

    # Get dictionary of floors for selected location
    building_dict, floor_dict = create_floor_dictionary(selected_location)

    # Get user input for selected building
    print('\n === Buildings ===')
    columns = 3
    num_buildings = len(floor_dict)
    num_spaces = len(str(num_buildings)) + 2  # add 2 extra spaces for dot and space
    num_rows = (num_buildings + columns - 1) // columns

    for row in range(num_rows):
        for col in range(columns):
            index = col * num_rows + row
            if index < num_buildings:
                building_str = f"{index+1}: {list(floor_dict.keys())[index]}"
                padded_str = building_str.ljust(num_spaces + 25)
                print(padded_str, end="")
        print()
    selected_index = input('\nEnter the number of the building you want to select: ')
    selected_building = list(floor_dict.keys())[int(selected_index)-1]

    # Create list of unique floor numbers for selected building
    unique_floor_ids = []
    for floor_number, floor_data in floor_dict[selected_building].items():
        floor_id = floor_data['id']
        if floor_id not in unique_floor_ids:
            unique_floor_ids.append(floor_id)

    # Print out list of floors for user to select from
    print('\n === Floors ===')
    for index, floor_number in enumerate(unique_floor_ids):
        level_str = num2words(index+1, ordinal=True)
        print(f'{index+1}: {level_str} level (id={floor_number})')

    # Get user input for selected floor
    selected_floor_index = input('\nEnter the number of the floor you want to select: ')
    selected_floor_id = unique_floor_ids[int(selected_floor_index)-1]

    # Print user selections
    print(f'\nYou selected:\n\tCampus: {selected_location}, \n\tBuilding: {selected_building}, {num2words(selected_floor_index, ordinal=True)} level (id={floor_number})')
    
    # Get user input for time interval/s
    print('\n === Data history length ===')
    print('Set num and length of time intervals (max length: 24hours)')    
    interval_length = input('\nEnter interval length in hours: ')
    interval_num = input('Enter number of interval to stitch together: ')
    
    # Get user input for filename
    filename = input('\nEnter filename to save data to: ')
    
    # Create interval list
    interval_list = create_time_interval(interval_num, interval_length)
    
    # Call get_data function with selected building and floor
    data = get_data(building_dict[selected_building],
                    selected_floor_id,
                    interval_num,
                    interval_list)
    
    # Export data to CSV
    output = save_to_csv(data, filename)
    
    # Display the final df
    print("\n\n===== subset of the data saved =====")
    print(tabulate(output.iloc[:10, :6], headers='keys', tablefmt='psql'))

if __name__ == "__main__":
    main()
    
