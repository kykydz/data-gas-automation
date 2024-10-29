import json

from helper.configuration import CSV_PATH, IS_RECONCILIATION
from helper.csv_file_parser_util import read_csv, randomize_and_partition, pick_current_used_segment, format_transaction_data
from helper.general_util import random_sleep
from helper.api import process_transaction, check_nik

def execute(rowData, nikResult, i, length_data):
    formatted_transaction_body = format_transaction_data(
            rowData['NIK'], nikResult['data'], rowData['Type'])
    result_transaction = process_transaction(formatted_transaction_body)
    print(f'\n Progress {i}/{length_data}')
    print(f'NIK: { rowData }\t Result: {json.dumps(result_transaction)}')
    print('\n')

def main():
    # Read CSV file and convert to JSON structure
    rawData = read_csv(CSV_PATH)
    
    selected_file_name = ''
    # Read or use all data if reconciliation
    if IS_RECONCILIATION:
        print('Start Reconciliation it is using all data')
        selected_file_name = CSV_PATH
    else:
        # randomize and partition the data for friday and saturday
        randomize_and_partition(rawData)
        selected_file_name = pick_current_used_segment()
    
    data = read_csv(selected_file_name)
    length_data = len(data)
    print(f'File segment: {selected_file_name}')
    print(f'Is Reconciliation: {IS_RECONCILIATION}')
    print(f'Processing data: {length_data}')

    random_sleep(5,10)

    i = 1
    for row in data:
        nikResult = check_nik(row['NIK'])

        # Execute only mikro if reconciliation
        if IS_RECONCILIATION:
            if row['Type'] == 'mikro':
                execute(row, nikResult, i, length_data)
        else:
            execute(row, nikResult, i, length_data)

        i = i + 1
        random_sleep(8, 15)

main()
