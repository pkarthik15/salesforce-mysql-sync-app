import pandas as pd
from salesforce import get_records
from db import save_data_to_table, truncate_table, get_upsert_method

def full_sync(session_id:str, server_url:str, query:str, table_name:str):
    print("Sync Started")
    query_url = f"""services/data/v59.0/query/?q={query}"""
    total_size = 0
    page_size = 0
    truncate_table(table_name)
    while(True):
        response = get_records(session_id, server_url, query_url)
        if(response.status_code == 200):
            response_json = response.json()
            total_size = response_json["totalSize"]
            page_size += len(response_json["records"])
            df = pd.DataFrame.from_dict(response_json["records"])
            df = df.drop('attributes', axis=1)
            save_data_to_table(df, table_name)
            print(f"fetched {page_size} records of {total_size}")
            if(response_json["done"] == True):
                break
            else:
                query_url = response_json["nextRecordsUrl"]
        else:
            break
    print("Sync Completed")


def update_sync(session_id:str, server_url:str, query:str, table_name:str):
    query_url = f"""services/data/v59.0/query/?q={query}"""
    total_size = 0
    page_size = 0
    method = get_upsert_method()
    while(True):
        response = get_records(session_id, server_url, query_url)
        if(response.status_code == 200):
            response_json = response.json()
            total_size = response_json["totalSize"]
            if(len(response_json["records"]) > 0):
                page_size += len(response_json["records"])
                print(f"Fetched {page_size} records of {total_size}")
                df = pd.DataFrame.from_dict(response_json["records"])
                df = df.drop('attributes', axis=1)
                save_data_to_table(df, table_name, method)
                print(f"Saved {page_size} records of {total_size}")
            else:
                print("No records to sync")
            if(response_json["done"] == True):
                break
            else:
                query_url = response_json["nextRecordsUrl"]
        else:
            break