import random
import time

def clean_json(data):
    if isinstance(data, dict):
        return {
            key: clean_json(value)
            for key, value in data.items()
            if value not in ("", None) and value != "null"
        }
    elif isinstance(data, list):
        return [clean_json(item) for item in data if item not in ("", None) and item != "null"]
    else:
        return data

def random_sleep(min_seconds=1, max_seconds=5):
    sleep_time = random.uniform(min_seconds, max_seconds)
    print(f"Sleeping for {sleep_time:.2f} seconds")
    time.sleep(sleep_time)