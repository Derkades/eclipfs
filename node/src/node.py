from flask import Flask, Response, request, abort, jsonify
from threading import Thread
from time import sleep
import schedule
import dsnapi
import threading
from os import environ as env
import os
from pathlib import Path
from requests.exceptions import RequestException
import requests
import random


app = Flask(__name__)


def verify_request_auth(typ):
    if 'node_token' not in request.args:
        abort(401, 'Missing node_token')

    if typ == 'read':
        token = env['TOKEN'][:64]
    elif typ == 'write':
        token = env['TOKEN'][64:]
    elif typ == 'full':
        token = env['TOKEN']
    else:
        raise Exception('invalid type argument')

    if not request.args['node_token'] == token:
        abort(403, 'Invalid node_token')


def get_chunk_path(chunk_token, mkdirs=False):
    if len(chunk_token) != 128:
        print('get_chunk_path fail: chunk token length is ', len(chunk_token))
        return None
    if '/' in chunk_token or '\\' in chunk_token or '.' in chunk_token:
        print('get_chunk_path fail: contains invalid characters')
        return None
    base = env['DATA_DIR']
    dir_1 = chunk_token[:2]
    dir_2 = chunk_token[2:4]
    if mkdirs:
        Path(os.path.join(base, dir_1, dir_2)).mkdir(parents=True, exist_ok=True)
    file_name = chunk_token[4:]
    return os.path.join(base, dir_1, dir_2, file_name)


def read_chunk(chunk_token):
    """
    Read chunk data from local disk

    Parameters:
        chunk_token: Chunk token
    Returns:
        Chunk data, or None if the chunk does not exist locally
    """
    path = get_chunk_path(chunk_token)
    if not path:
        return None
    print('path', path)
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as file:
        data = file.read()
    return data


def create_chunk(chunk_token, data):
    """
    Write chunk data to local disk, overwriting if the chunk already exists

    Parameters:
        chunk_token: Chunk token
        data: Chunk data
    Returns:
        success boolean
    """
    path = get_chunk_path(chunk_token, mkdirs=True)
    if not path:
        print('create_chunk fail, path is None')
        return False
    # print('create_chunk: path exists:', os.path.exists(path))
    # print('path', path)
    # print('data length:', len(data))
    if len(data) > 1000000:
        print('create_chunk fail, data too large')
        return False
    with open(path, 'wb') as file:
        file.write(data)
    return True


@app.route('/ping', methods=['GET'])
def ping():
    verify_request_auth('full')
    return Response(response='pong', content_type="text/plain")


@app.route('/upload', methods=['POST'])
def upload():
    verify_request_auth('write')

    if 'chunk_token' not in request.args:
        abort(400, 'Missing chunk_token')

    if request.content_type != 'application/octet-stream':
        abort(400, 'Request content type must be application/octet-stream')

    if request.data == b'':
        abort(400, 'Request body is empty')

    chunk_token = request.args['chunk_token']

    if not create_chunk(chunk_token, request.data):
        abort(500, 'Unable to write chunk data to file')

    (success, response, error_message) = dsnapi.notify_chunk_uploaded(chunk_token, len(request.data))
    if success:
        return Response('ok', content_type='text/plain')
    else:
        # chunk is already written to disk and not accounted for on metaserver
        # metaserver will try to replicate and garbage collection will remove the
        # chunk from this chunkserver.
        print('error sending chunk upload notification to metaserver:', response, error_message)
        abort(500, 'Unable to send chunk upload notification to metaserver')


@app.route('/replicate', methods=['POST'])
def replicate():
    verify_request_auth('full')

    if 'chunk_token' not in request.args:
        abort(400, 'Missing chunk_token')

    if 'address' not in request.args:
        abort(400, 'Missing address')

    chunk_token = request.args['chunk_token']

    data = read_chunk(chunk_token)
    if not data:
        abort(404, 'Chunk not found. Is the token valid and of the correct length?')

    address = request.args['address']
    r = requests.post(address, headers={'Content-Type': 'application/octet-stream'}, data=data)
    if r.status_code == 200:
        return Response('ok', content_type='text/plain')
    else:
        abort(500, r.text)


@app.route('/download', methods=['GET'])
def download():
    verify_request_auth('read')

    if 'chunk_token' not in request.args:
        abort(400, 'Missing chunk_token')

    data = read_chunk(request.args['chunk_token'])
    if data:
        return Response(data, content_type='application/octet-stream')
    else:
        return abort(404, 'Chunk not found. Is the token valid and of the correct length?')


def announce():
    try:
        (success, response, error_message) = dsnapi.announce()
        if not success:
            print('Unable to contact metaserver:', response, error_message)
    except RequestException as e:
        print('Unable to contact metaserver:', e)


def ls(path, is_file=False):
    if not os.path.exists(path):
        return []
    return [e for e in os.listdir(path) if is_file == os.path.isfile(os.path.join(path, e))]


def dir_empty(dir_path):
    try:
        next(os.scandir(dir_path))
        return False
    except StopIteration:
        return True


def garbage_collect(count=500):
    base = env['DATA_DIR']
    subdirs = ls(base)
    if len(subdirs) < count:
        print('Skipping garbage collection, this node is not storing that much data.')
        return

    directories = [os.path.join(base, choice) for choice in random.sample(subdirs, count)]
    print(f'Starting garbage collection, scanning {count} dirs')
    # print('Garbage collecting dirs', directories)
    chunk_tokens = []
    for directory1 in directories:
        subdirs = ls(directory1)
        if len(subdirs) == 0:
            # print('directory empty, delete')
            os.rmdir(os.path.join(base, directory1))
        else:
            for directory2 in subdirs:
                subfiles = ls(os.path.join(directory1, directory2), is_file=True)
                if len(subfiles) == 0:
                    # print('directory empty, delete')
                    os.rmdir(os.path.join(directory1, directory2))
                else:
                    for file2 in subfiles:
                        token = os.path.basename(directory1) + directory2 + file2
                        chunk_tokens.append(token)

    print('Making request to metaserver')
    # r = requests.get('/node/checkGarbage', headers=dsnapi.HEADERS, data={chunks: chunk_tokens})
    (success, response, error_message) = dsnapi.get('checkGarbage', data={'chunks': chunk_tokens})
    if not success:
        print('garbage collection error', response, error_message)
        return

    garbage_tokens = response['garbage']

    print('Found garbage', len(garbage_tokens), '/', len(chunk_tokens), '- deleting now...')
    deleted_files = 0
    deleted_dirs = 0
    for garbage_token in garbage_tokens:
        path = get_chunk_path(garbage_token)
        if not path:
            # print('invalid', path)
            pass
        elif os.path.exists(path):
            # print('delete', path)
            os.remove(path)
            deleted_files += 1
            parent = Path(path).parent.absolute()
            if dir_empty(parent):
                parent2 = Path(parent).parent.absolute()
                # print('delete parent')
                os.rmdir(parent)
                deleted_dirs += 1
                if dir_empty(parent2):
                    # print('delete parent2')
                    os.rmdir(parent2)
                    deleted_dirs += 1
        else:
            pass
            # print('skip', path)

    print(f'Done, deleted {deleted_files} files and {deleted_dirs} directories')



def timers():
    announce()
    schedule.every(7).to(9).seconds.do(announce)
    schedule.every(30).to(60).seconds.do(garbage_collect)
    while True:
        schedule.run_pending()
        sleep(1)


t = threading.Thread(target=timers, args=[])
t.daemon = True # required to exit nicely on SIGINT
t.start()
