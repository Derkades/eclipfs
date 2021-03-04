import requests
import json as jsonlib
from os import environ

HEADERS = {
    "X-DSN-NodeToken": environ['TOKEN']
}

def announce():
    data = {
        'version': 'dev',
        'free': 0,
        'address': environ['OWN_ADDRESS'],
        'quota': 100,
        'name': 'test',
        'label': environ['LABEL'],
    }

    r = requests.post(environ['METASERVER_ADDRESS'] + '/node/announce', headers=HEADERS, json=data)
    if r.status_code == 200:
        try:
            json = r.json()
        except jsonlib.JSONDecodeError:
            return (False, None, 'Json decode error for ' + r.text)

        if 'error' in json:
            error_code = json['error']
            return (False, error_code, json['error_message'] if 'error_message' in json else 'No error message available')
        else:
            return (True, json, None)
    else:
        return (False, None, f'Status code {r.status_code}')


def notify_chunk_uploaded(chunk_token, chunk_size):
    data = {
        'chunk_token': chunk_token,
        'chunk_size': chunk_size,
    }
    r = requests.post(environ['METASERVER_ADDRESS'] + '/node/notifyChunkUploaded', headers=HEADERS, json=data)
    if r.status_code == 200:
        try:
            json = r.json()
        except jsonlib.JSONDecodeError:
            return (False, None, 'Json decode error for ' + r.text)

        if 'error' in json:
            error_code = json['error']
            return (False, error_code, json['error_message'] if 'error_message' in json else 'No error message available')
        else:
            return (True, json, None)
    else:
        return (False, None, f'Status code {r.status_code} - ' + str(r.content))
