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
import shutil


app = Flask(__name__)


def verify_request_auth(typ):
    if 'node_token' not in request.args:
        abort(401, 'Missing node_token')

    if len(env['TOKEN']) != 32:
        raise Exception('Node token should be 32 characters long')

    if typ == 'read':
        token = env['TOKEN'][:16]
    elif typ == 'write':
        token = env['TOKEN']
    else:
        raise Exception('invalid type argument')

    if not request.args['node_token'] == token:
        abort(403, 'Invalid node_token')


def get_chunk_path(inode, index, mkdirs=False):
    base = env['DATA_DIR']
    dir_1 = str(inode // 1000)
    dir_2 = str(inode % 1000)
    dirs = os.path.join(base, dir_1, dir_2)
    if mkdirs:
        Path(dirs).mkdir(parents=True, exist_ok=True)
    return os.path.join(dirs, str(index) + '.efs')


def read_chunk(inode, index):
    """
    Read chunk data from local disk

    Parameters:
        inode: File id / inode
        index: Chunk index
    Returns:
        Chunk data, or None if the chunk does not exist locally
    """
    path = get_chunk_path(inode, index)
    if not path:
        return None
    print('path', path)
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as file:
        data = file.read()
    return data


def create_chunk(inode, index, data):
    """
    Write chunk data to local disk, overwriting if the chunk already exists

    Parameters:
        inode: File id / inode
        index: Chunk index
    Returns:
        success boolean
    """
    path = get_chunk_path(inode, index, mkdirs=True)
    if not path:
        print('create_chunk fail, path is None')
        return False

    if len(data) > 1000000:
        print('create_chunk fail, data too large')
        return False
    with open(path, 'wb') as file:
        file.write(data)
    return True


@app.route('/ping', methods=['GET'])
def ping():
    verify_request_auth('write')
    return Response(response='pong', content_type="text/plain")


@app.route('/upload', methods=['POST'])
def upload():
    verify_request_auth('write')

    if 'file' not in request.args or 'index' not in request.args:
        abort(400, 'Missing file or index parameter')

    if request.content_type != 'application/octet-stream':
        abort(400, 'Request content type must be application/octet-stream')

    if request.data == b'':
        abort(400, 'Request body is empty')

    inode = int(request.args['file'])
    index = int(request.args['index'])

    if not create_chunk(inode, index, request.data):
        abort(500, 'Unable to write chunk data to file')

    (success, response, error_message) = dsnapi.notify_chunk_uploaded(inode, index, len(request.data))
    if success:
        return Response('ok', content_type='text/plain')
    else:
        # chunk is already written to disk and not accounted for on metaserver
        # it will stay on this node until the file is deleted, minor bug.
        print('error sending chunk upload notification to metaserver:', response, error_message)
        abort(500, 'Unable to send chunk upload notification to metaserver')


@app.route('/download', methods=['GET'])
def download():
    verify_request_auth('read')

    if 'file' not in request.args or 'index' not in request.args:
        abort(400, 'Missing file or index parameter')

    inode = int(request.args['file'])
    index = int(request.args['index'])
    data = read_chunk(inode, index)
    if data is not None:
        return Response(data, content_type='application/octet-stream')
    else:
        return abort(404, 'Chunk not found. Is the token valid and of the correct length?')


@app.route('/replicate', methods=['POST'])
def replicate():
    verify_request_auth('write')

    if 'file' not in request.args or 'index' not in request.args:
        abort(400, 'Missing file or index parameter')

    if 'address' not in request.args:
        abort(400, 'Missing address')

    inode = int(request.args['file'])
    index = int(request.args['index'])
    data = read_chunk(inode, index)

    if data is None:
        abort(404, 'Chunk not found. Is the token valid and of the correct length?')

    address = request.args['address']
    r = requests.post(address, headers={'Content-Type': 'application/octet-stream'}, data=data)
    if r.status_code == 200:
        return Response('ok', content_type='text/plain')
    else:
        abort(500, r.text)


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


def garbage_collect():
    base = env['DATA_DIR']
    subdirs = ls(base)
    if len(subdirs) == 0:
        print('Skipping garbage collection, this node is not storing any data.')
        return

    chosen_dir = random.choice(subdirs)
    print('Garbage collecting dir', chosen_dir)

    files = [int(d) for d in ls(os.path.join(base, chosen_dir))]

    if len(files) == 0:
        print('No files in directory')
        return

    print('Making request to metaserver', files)
    (success, response, error_message) = dsnapi.get('checkGarbage', data={'files': files})
    if not success:
        print('garbage collection error', response, error_message)
        return

    garbage = response['garbage']
    for f in garbage:
        path = os.path.join(base, chosen_dir, str(f))
        print('Deleting', path)
        shutil.rmtree(path)


def timers():
    announce()
    schedule.every(7).to(9).seconds.do(announce)
    schedule.every(300).to(600).seconds.do(garbage_collect)
    while True:
        schedule.run_pending()
        sleep(1)


t = threading.Thread(target=timers, args=[])
t.daemon = True # required to exit nicely on SIGINT
t.start()
