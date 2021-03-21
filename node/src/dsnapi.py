import requests
import json as jsonlib
from os import environ as env
import shutil

HEADERS = {
    "X-DSN-NodeToken": env['TOKEN']
}

def announce():
    _, _, free = shutil.disk_usage(env['DATA_DIR'])

    print('Free space:', free)
    reservation = int(env['RESERVATION']) * 1_000_000_000 if 'RESERVATION' in env else 0
    print('Reservation:', reservation)
    new_free = abs(free - reservation)
    print(f'Free space, accounting for reservation: {new_free} ({new_free/1_000_000_000} GB)')

    data = {
        'version': 'dev',
        'address': env['OWN_ADDRESS'],
        'free': new_free,
        'quota': 0 # not used right now
    }

    r = requests.post(env['METASERVER_ADDRESS'] + '/node/announce', headers=HEADERS, json=data)
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
        print(r.text)
        return (False, None, f'Status code {r.status_code}')


def notify_chunk_uploaded(chunk_token, chunk_size):
    data = {
        'chunk_token': chunk_token,
        'chunk_size': chunk_size,
    }
    r = requests.post(env['METASERVER_ADDRESS'] + '/node/notifyChunkUploaded', headers=HEADERS, json=data)
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


def get(method, data):
    r = requests.post(env['METASERVER_ADDRESS'] + '/node/' + method, headers=HEADERS, json=data)
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
        print(r.text)
        return (False, None, f'Status code {r.status_code}')
