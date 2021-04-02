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
import threading
import hashlib
# import binascii


app = Flask(__name__)


fs_lock = threading.Lock()


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


def get_chunk_path(chunk_id, mkdirs=False):
    base = env['DATA_DIR']
    dir_1 = str(chunk_id // 1_000_000)
    dir_2 = str(chunk_id % 1_000_000)
    dir_3 = str(chunk_id // 1_000)
    file_name = str(chunk_id % 1_000) + '.efs'

    dirs = os.path.join(base, dir_1, dir_2, dir_3)
    if mkdirs:
        Path(dirs).mkdir(parents=True, exist_ok=True)
    return os.path.join(dirs, file_name)


def get_temp_path(temp_id):
    # prevent directory traversal by ensuring temp_id is int
    return '/tmp/eclipfs-chunkserver-' + str(int(temp_id)) + '.efs'


def read_chunk(chunk_id):
    """
    Read chunk data from local disk

    Parameters:
        inode: File id / inode
        index: Chunk index
    Returns:
        Chunk data, or None if the chunk does not exist locally
    """
    path = get_chunk_path(chunk_id)
    # print('path', path)
    if not os.path.exists(path):
        print('WARNING: path', path, 'not found!')
        return None
    with open(path, 'rb') as file:
        data = file.read()
    return data


# def create_chunk(chunk_id, data):
#     """
#     Write chunk data to local disk, overwriting if the chunk already exists

#     Parameters:
#         inode: File id / inode
#         index: Chunk index
#     Returns:
#         success boolean
#     """
#     path = get_chunk_path(chunk_id, mkdirs=True)
#     if not path:
#         print('create_chunk fail, path is None')
#         return False

#     if len(data) > 1000000:
#         print('create_chunk fail, data too large')
#         return False
#     with open(path, 'wb') as file:
#         file.write(data)
#     return True


@app.route('/ping', methods=['GET'])
def ping():
    verify_request_auth('write')
    return Response(response='pong', content_type="text/plain")


@app.route('/upload', methods=['POST'])
def upload():
    verify_request_auth('write')

    # if 'chunk' not in request.args:
    #     abort(400, "Missing 'chunk' parameter")

    if 'id' not in request.args:
        abort(400, 'Missing id parameter')

    if request.content_type != 'application/octet-stream':
        abort(400, 'Request content type must be application/octet-stream')

    if request.data == b'':
        abort(400, 'Request body is empty')

    # inode = int(request.args['file'])
    # index = int(request.args['index'])
    # chunk_id = int(request.args['chunk'])
    temp_id = int(request.args['id'])

    data = request.data

    if len(data) > 1000000:
        # print('create_chunk fail, data too large')
        abort(404, 'Data too large')

    fs_lock.acquire()
    # if not create_chunk(chunk_id, request.data):
    #     fs_lock.release()
    #     abort(500, 'Unable to write chunk data to file')

    temp_path = get_temp_path(temp_id)
    print('writing to', temp_path)
    with open(temp_path, 'wb') as file:
        file.write(data)

    # (success, response, error_message) = dsnapi.notify_chunk_uploaded(chunk_id, len(request.data))
    # if success:
    fs_lock.release()
    return Response('ok', content_type='text/plain')
    # else:
    #     print('error sending chunk upload notification to metaserver:', response, error_message)
    #     fs_lock.release()
    #     abort(500, 'Unable to send chunk upload notification to metaserver')


@app.route('/finalize', methods=['POST'])
def finalize():
    if not request.json:
        abort(400, 'request body not valid json')

    if 'temp_id' not in request.json:
        abort(400, 'Missing temp_id parameter')
    if 'chunk_id' not in request.json:
        abort(400, 'Missing chunk parameter')

    temp_id = int(request.json['temp_id'])
    chunk_id = int(request.json['chunk_id'])

    temp_path = get_temp_path(temp_id)

    fs_lock.acquire()
    if not os.path.isfile(temp_path):
        fs_lock.release()
        print('temp file does not exist', temp_path)
        abort(404, 'temp file does not exist')

    new_path = get_chunk_path(chunk_id, mkdirs=True)

    shutil.move(temp_path, new_path)
    print('tmp', os.listdir('/tmp'))
    fs_lock.release()
    return Response('ok', content_type='text/plain')


@app.route('/download', methods=['GET'])
def download():
    verify_request_auth('read')

    if 'chunk' not in request.args:
        abort(400, "Missing 'chunk' parameter")

    # inode = int(request.args['file'])
    # index = int(request.args['index'])
    chunk_id = int(request.args['chunk'])

    fs_lock.acquire()
    data = read_chunk(chunk_id)
    fs_lock.release()
    if data is not None:
        return Response(data, content_type='application/octet-stream')
    else:
        return abort(404, 'Chunk not found. Is the token valid and of the correct length?')


@app.route('/replicate', methods=['POST'])
def replicate():
    verify_request_auth('write')

    if 'chunk' not in request.args:
        abort(400, "Missing 'chunk' parameter")

    if 'checksum' not in request.args:
        abort(400, "Missing 'checksum' parameter")

    if 'address' not in request.args:
        abort(400, 'Missing address')

    # inode = int(request.args['file'])
    # index = int(request.args['index'])
    chunk_id = int(request.args['chunk'])
    checksum = request.args['checksum']
    address = request.args['address']

    # fs_lock.acquire()
    # data = read_chunk(chunk_id)
    # fs_lock.release()

    # if data is None:
    #     abort(404, 'Chunk not found. Is the token valid and of the correct length?')


    try:
        r = requests.post(address, headers={'Content-Type': 'application/octet-stream'})
        if r.status_code == 200:
            data = r.content
            if hashlib.md5(data).hexdigest() != checksum:
                abort(500, 'Checksum mismatch', checksum, 'data len', len(data))

            fs_lock.acquire()
            path = get_chunk_path(chunk_id, mkdirs=True)

            print('replication - writing to', path)
            with open(path, 'wb') as file:
                file.write(data)

            fs_lock.release()

            return Response('ok', content_type='text/plain')
        else:
            abort(500, r.text)
    except RequestException as e:
        abort(500, (address, e))


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


# def dir_empty(dir_path):
#     try:
#         next(os.scandir(dir_path))
#         return False
#     except StopIteration:
#         return True


# def garbage_collect():
#     fs_lock.acquire()
#     base = env['DATA_DIR']
#     subdirs = ls(base)
#     if len(subdirs) == 0:
#         print('Skipping garbage collection, this node is not storing any data.')
#         fs_lock.release()
#         return

#     chosen_dir = random.choice(subdirs)
#     print('Garbage collecting dir', chosen_dir)

#     files = [int(d) for d in ls(os.path.join(base, chosen_dir))]

#     if len(files) == 0:
#         print('No files in directory')
#         fs_lock.release()
#         return

#     print('Making request to metaserver for', len(files), 'files')
#     (success, response, error_message) = dsnapi.get('checkGarbage', data={'files': files})
#     if not success:
#         print('garbage collection error', response, error_message)
#         fs_lock.release()
#         return

#     garbage = response['garbage']
#     for f in garbage:
#         path = os.path.join(base, chosen_dir, str(f))
#         print('Deleting', path)
#         shutil.rmtree(path)

#     fs_lock.release()


def timers():
    announce()
    schedule.every(7).to(9).seconds.do(announce)
    # schedule.every(300).to(600).seconds.do(garbage_collect)
    while True:
        schedule.run_pending()
        sleep(1)


t = threading.Thread(target=timers, args=[])
t.daemon = True # required to exit nicely on SIGINT
t.start()
