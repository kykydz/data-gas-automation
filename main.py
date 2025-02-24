import json

from helper.configuration import FILE_RECON, IS_RECONCILIATION, TEST_CSV_PATH, BID_TYPES_TO_RECONCILIATE
from helper.csv_file_parser_util import read_csv, format_transaction_data, file_selection
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
    selected_file_name = file_selection(TEST_CSV_PATH)
    if IS_RECONCILIATION:
        selected_file_name = FILE_RECON
        print('\nReconciliation mode is detected, continue?')

    data = read_csv(selected_file_name) 
    length_data = len(data)

    print(f'\nFile segment: {selected_file_name}')
    print(f'Is Reconciliation: {IS_RECONCILIATION}')
    print(f'Processing data: {length_data}')

    random_sleep(5,10)

    i = 1
    for row in data:
        nikResult = check_nik(row['NIK'])

        # Execute only mikro if reconciliation
        if IS_RECONCILIATION:
            if row['Type'] in BID_TYPES_TO_RECONCILIATE:
                execute(row, nikResult, i, length_data)
        else:
            execute(row, nikResult, i, length_data)

        i = i + 1
        random_sleep(20, 30)

main()
