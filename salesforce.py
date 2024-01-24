import requests
from typing import Tuple


def get_auth_token(environment:str, username:str, password:str, client_id:str, client_secret:str) -> Tuple[str, str]:
    session_id = ""
    server_url = ""

    if(environment == 'production'):
        login_url = "https://login.salesforce.com/services/oauth2/token"
    elif(environment == 'sandbox'):
        login_url = "https://test.salesforce.com/services/oauth2/token"
    else:
        login_url = ""

    payload = f"""username={username}&password={password}&client_id={client_id}&client_secret={client_secret}&grant_type=password"""
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    if login_url != '':
        response = requests.post(login_url, headers=headers, data=payload)
        
        if(response.status_code == 200):
            response_json = response.json()
            session_id = response_json["access_token"]
            server_url = response_json["instance_url"]+"/".strip()

    return session_id, server_url


def get_records(session_id:str, server_url:str, query_url: str):
    
    headers = {
        "Authorization": "OAuth " + session_id
    }
   
    object_url = server_url + query_url

    response = requests.get(object_url, headers=headers)
    
    return response

