import config
import requests
import base64
import json as jsonlib

def get_headers():
    return {
        "X-DSN-Username": base64.b64encode(config.USERNAME.encode()),
        "X-DSN-Password": base64.b64encode(config.PASSWORD.encode())
    }

def get(api_method, params={}):
    url = f"{config.METASERVER}/client/{api_method}"
    print("Making request", url, params)
    r = requests.get(url, headers=get_headers(), params=params)
    if r.status_code == 200:
        json = r.json()
        if 'error' in json:
            print('API error, error code is printed below')
            error_code = json['error']
            error_message = json['error_message'] if 'error_message' in json else 'No error message available'
            print(f'{error_code}: {error_message}')
            return (False, error_code)
        else:
            return (True, json)
    else:
        print(f'Status code {r.status_code}, response is printed below.')
        print(r.text)
        print('Request params below')
        print(params)

def post(api_method, data):
    url = f"{config.METASERVER}/client/{api_method}"
    print("Making request", url, data)
    r = requests.post(url, headers=get_headers(), json=data)
    if r.status_code == 200:
        try:
            json = r.json()
        except jsonlib.JSONDecodeError as e:
            print('Json decode error', e)
            print('Response as text:', r.text)

        if 'error' in json:
            print('API error, error code is printed below')
            error_code = json['error']
            error_message = json['error_message'] if 'error_message' in json else 'No error message available'
            print(f'{error_code}: {error_message}')
            return (False, error_code)
        else:
            return (True, json)
    else:
        print(f'Status code {r.status_code}, response is printed below.')
        print(r.text)
        print('Request data below')
        print(data)
