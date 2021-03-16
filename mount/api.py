import config
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from requests_toolbelt import sessions
import base64
import json as jsonlib

DEFAULT_TIMEOUT = 30

class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


retries = Retry(
    total=200,
    status_forcelist=[429],
    method_whitelist=['GET', 'POST'],
    backoff_factor=0.3
)

http = sessions.BaseUrlSession(base_url=config.METASERVER)
adapter = TimeoutHTTPAdapter(max_retries=retries)
http.mount("http://", adapter)
http.mount("https://", adapter)

def get_headers():
    return {
        "X-DSN-Username": base64.b64encode(config.USERNAME.encode()),
        "X-DSN-Password": base64.b64encode(config.PASSWORD.encode())
    }

def get(api_method, params={}):
    url = '/client/' + api_method
    print("Making request", url, params)
    r = http.get(url, headers=get_headers(), params=params)
    if r.status_code == 200:
        json = r.json()
        if 'error' in json:
            print('API error, error code is printed below. (this does not always indicate a bug, sometimes API errors are normal)')
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
    url = '/client/' + api_method
    print("Making request", url, data)
    r = http.post(url, headers=get_headers(), json=data)
    if r.status_code == 200:
        try:
            json = r.json()
        except jsonlib.JSONDecodeError as e:
            print('Json decode error', e)
            print('Response as text:', r.text)

        if 'error' in json:
            print('API error, error code is printed below (this does not always indicate a bug, sometimes API errors are normal)')
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
