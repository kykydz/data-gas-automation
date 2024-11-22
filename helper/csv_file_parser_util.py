import csv
import json
import random
import datetime

from helper.general_util import clean_json
from helper.configuration import MIKRO, PRODUCT_ID, FILE_SEGMENT_1, FILE_SEGMENT_2, CSV_PATH, IS_RECONCILIATION, N_GAS_DATA

from helper.general_util import random_sleep

def read_csv(path):
    data = []
    with open(path, mode='r') as file:
        # Create a CSV reader object
        csv_reader = csv.reader(file)
        next(csv_reader)

        i = 0
        for row in csv_reader:
            NIK, Name, Address, Type = row
            row_data = {
                'NIK': NIK,
                'Name': Name,
                'Address': Address,
                'Type': Type
            }
            # print(json.dumps(row_data))
            data.append(row_data)
    return data

def randomize_and_partition(arr, n):
    # Shuffle array
    random.shuffle(arr)
    
    # take n data points
    arr = arr[:n]

    # Determine the split point (between 50% and 65% of the array length)
    split_point = random.randint(int(len(arr) * 0.50), int(len(arr) * 0.65))
    # Break the array into two segments
    segment1 = arr[:split_point]
    segment2 = arr[split_point:]

    print(f'Segment 1 length: {len(segment1)}')
    print(f'Segment 2 length: {len(segment2)}')

    # Write the segments to CSV files
    write_segments_to_csv(segment1, segment2)
    return segment1, segment2

def pick_current_used_segment(segment_file_used):
    if segment_file_used:
        return segment_file_used

    today = datetime.datetime.today().strftime('%A')
    file_name = ''
    if today == 'Friday':
        file_name = FILE_SEGMENT_1
        print("Today is Friday use segment 1")
    elif today == 'Saturday':
        file_name = FILE_SEGMENT_2
        print("Today is Saturday use segment 2")
    else:
        print("Today is neither Friday nor Saturday. No file will be created.")
        file_name = FILE_SEGMENT_2
        # return
    return file_name

def write_segments_to_csv(segment1, segment2):
    # Function to write a segment to a CSV file
    def write_to_csv(filename, segment):
        with open(filename, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['NIK', 'Name', 'Address', 'Type'])
            writer.writeheader()
            writer.writerows(segment)
    
    # Write each segment to its respective CSV file
    write_to_csv(FILE_SEGMENT_1, segment1)
    write_to_csv(FILE_SEGMENT_2, segment2)


def format_transaction_data(nik, check_nik_result, type):
    customer_type = check_nik_result['customerTypes'][0]['name']

    type_buyer = 'Rumah Tangga'
    quantity = 1
    sourceTypeId = 1
    familyId = check_nik_result.get('familyId', None)

    if type == 'mikro' or customer_type == MIKRO:
        type_buyer = 'Usaha Mikro'
        quantity = 2
        sourceTypeId = 99
        familyId = ''

    data = {
        "products": [
            {
                "productId": PRODUCT_ID,
                "quantity": quantity
            }
        ],
        "geoTagging": "",
        "inputNominal": 15500,
        "change": 0,
        "paymentType": "cash",
        "subsidi": {
            "nik": nik,
            "IDValidation": "",
            "familyId": check_nik_result.get('familyId', None),
            "category": type_buyer,
            "sourceTypeId": sourceTypeId,
            "nama": check_nik_result['name'],
            "noHandPhoneKPM": check_nik_result['phoneNumber'],
            "channelInject": check_nik_result['channelInject'],
            "pengambilanItemSubsidi": [
                {
                    "item": "ELPIJI",
                    "quantitas": quantity,
                    "potongan_harga": 0
                }
            ]
        }
    }

    cleaned_data = clean_json(data)
    print(json.dumps(data))
    return cleaned_data

def file_selection(default_selected_file_name):
    # MODE: RECONCILIATION
    if IS_RECONCILIATION:
        print('Start Reconciliation it is using all data')
        return CSV_PATH
    
    # MODE: TESTING
    if default_selected_file_name != None:
        isValidCSVFile = default_selected_file_name.startswith("file/") and default_selected_file_name.endswith(".csv")
        if isinstance(default_selected_file_name, str) and isValidCSVFile:
            return default_selected_file_name
    
    # MODE: NORMAL
    rawData = read_csv(CSV_PATH)
    
    # check if this is saturday do not randomize
    today = datetime.datetime.today().strftime('%A')
    if today == 'Saturday':
        print("Today is Saturday , continue?")
        random_sleep(6,10)
        selected_file_name = pick_current_used_segment(default_selected_file_name)
        return selected_file_name
    else:
        print("Today is Friday , randomizing and partitioning...")
        random_sleep(6,10)
        # randomize and partition the data for friday
        randomize_and_partition(rawData, N_GAS_DATA)
        selected_file_name = pick_current_used_segment(default_selected_file_name)

        return selected_file_name