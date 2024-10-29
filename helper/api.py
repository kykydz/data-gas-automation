import requests

from helper.general_util import  random_sleep
from helper.configuration import BASIC_AUTH

def process_transaction(body):
    """
    Process a transaction with the given body.

    The function will retry up to 3 times if the connection is aborted.

    Args:
        body (dict): The JSON body of the transaction.

    Returns:
        dict: The JSON response of the transaction.
    """
    retry_count = 0
    while retry_count < 3:
        try:
            url = 'https://api-map.my-pertamina.id/general/v1/transactions'
            headers = {
                "Authorization": f"{BASIC_AUTH}"
            }
            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if str(e).lower().find('connection aborted') != -1:
                print(f"An error occurred: {e}. Retrying...")
                retry_count += 1
                random_sleep(5,7)
            else:
                print(f"An error occurred: {e}")
                return None
            
def check_nik(nik):
    print("nik ==> ", nik)
    try:
        url = 'https://api-map.my-pertamina.id/customers/v1/verify-nik?nationalityId=' + nik
        headers = {
            "Authorization": f"{BASIC_AUTH}"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
            
def get_products():
    try:
        url = 'https://api-map.my-pertamina.id/general/v2/products'
        headers = {
            "Authorization": f"{BASIC_AUTH}"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"An error occurred: {e}")
        return None