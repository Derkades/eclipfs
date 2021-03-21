#!/usr/bin/env python3
import os
import sys

import pyfuse3
import errno
import stat
import time
import sqlite3
import logging
from collections import defaultdict
from pyfuse3 import FUSEError
from argparse import ArgumentParser
import trio

import threading
import config
import api
from inode import Inode

import requests
from requests.exceptions import RequestException
import hashlib
import time

import base64
from Crypto.Cipher import AES

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

log = logging.getLogger()

class Operations(pyfuse3.Operations):
    supports_dot_lookup = True
    enable_writeback_cache = True

    def __init__(self, encryption_key):
        super(Operations, self).__init__()
        self.db = sqlite3.connect(':memory:')
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self.inode_open_count = defaultdict(int)
        self.init_tables()
        self.read_cache_lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.encryption_key = encryption_key


    def init_tables(self):
        self.cursor.execute("""
        CREATE TABLE write_buffer (
            inode           BIGINT NOT NULL,
            chunk_index     INTEGER NOT NULL,
            data            bytea NOT NULL,
            last_update     BIGINT NOT NULL,
            PRIMARY KEY(inode, chunk_index),
            UNIQUE(inode, chunk_index)
        )
        """)

        self.cursor.execute("""
        CREATE TABLE read_cache (
            inode           BIGINT NOT NULL,
            chunk_index     INTEGER NOT NULL,
            data            bytea NOT NULL,
            time            BIGINT NOT NULL,
            UNIQUE(inode, chunk_index)
        )
        """)

    def _get_row(self, *a, **kw):
        self.cursor.execute(*a, **kw)
        try:
            row = next(self.cursor)
        except StopIteration:
            raise NoSuchRowError()
        try:
            next(self.cursor)
        except StopIteration:
            pass
        else:
            raise NoUniqueValueError()

        return row


    def _get_cipher(self, inode, chunk_index):
        key = self.encryption_key
        # initial value needs to be 16 bytes, inode is a long (8 bytes) padded to 12 and chunk_index is a 4 byte int
        iv = inode.to_bytes(12, byteorder='big') + chunk_index.to_bytes(4, byteorder='big')
        return AES.new(key, AES.MODE_CFB, iv)


    def _clear_write_buffer(self, force=False):
        print('clear write buffer', force)

        self.write_lock.acquire()

        if not force:
            num_entries = self._get_row('SELECT COUNT(*) FROM write_buffer')[0]
            print('Write buffer contains', num_entries, 'entries')
            if num_entries < 10:
                print('skip clearing write buffer')
                self.write_lock.release()
                return

        try:
            inode = self._get_row('SELECT inode FROM write_buffer LIMIT 1')['inode']
        except NoSuchRowError:
            print('Write queue is empty')
            inode = None

        failure = True # so the loop runs once
        while failure:
            failure = False

            while inode:
                # Note: one inode in the write buffer may appear multiple times, with different chunk indices. We only selet one
                (data, chunk_index) = self._get_row('SELECT data,chunk_index FROM write_buffer WHERE inode=? LIMIT 1', (inode,))

                encrypted_data = self._get_cipher(inode, chunk_index).encrypt(data)
                checksum = hashlib.md5(encrypted_data).hexdigest()
                size = len(encrypted_data)

                print('making chunkTransfer request', inode, chunk_index, checksum, size, inode)

                (success, response) = api.post('chunkTransfer', data={'file': inode, 'chunk': chunk_index, 'type': 'upload', 'checksum': checksum, 'size': size})

                if not success:
                    print('FAILED TO REQUEST CHUNK TRANSFER', inode, chunk_index)
                    failure = True
                    break

                url = response['url']

                try:
                    r = requests.post(url, data=encrypted_data, headers={'Content-Type':'application/octet-stream'})
                except RequestException:
                    print('Failed to connect to node')
                    failure = True
                    break

                if r.status_code != 200:
                    print('FAILED TO UPLOAD CHUNK status code', r.status_code, r.content)
                    failure = True
                    break

                # TODO in the future: instead of simply removing from write buffer, move to read cache

                # Remove the one chunk inode+chunk_index we just uplaoded. If there are different chunk indices for this inode in the write buffer,
                # the same inode may be picked again for the next loop.
                print('upload appears successful, removing from write buffer')
                self.cursor.execute('DELETE FROM write_buffer WHERE inode=? AND chunk_index=?', (inode, chunk_index))

                try:
                    inode = self._get_row('SELECT inode FROM write_buffer LIMIT 1')['inode']
                    print('Not done yet, there are more entries in the write buffer...')
                except NoSuchRowError:
                    inode = None

            if failure:
                print('Errors were encountered while clearing the write buffer. Trying again in 5 seconds...')
                time.sleep(5)
            else:
                print('done clearing write buffer')

        self.write_lock.release()


    async def lookup(self, inode_p, name, ctx=None):
        """
        Look up a directory entry by name and get its attributes.

        This method should return an EntryAttributes instance for the directory entry name in the
        directory with inode parent_inode.

        If there is no such entry, the method should either return an EntryAttributes instance with
        zero st_ino value (in which case the negative lookup will be cached as specified by
        entry_timeout), or it should raise FUSEError with an errno of errno.ENOENT (in this case
        the negative result will not be cached).

        ctx will be a RequestContext instance.

        The file system must be able to handle lookups for . and .., no matter if these entries are
        returned by readdir or not.

        (Successful) execution of this handler increases the lookup count for the returned inode by one.
        """

        if name == '.':
            inode = inode_p
        elif name == '..':
            inode = self._get_parent(inode)
        else:
            info = Inode.by_name(inode_p, name)
            return self._getattr(info, ctx)


    async def getattr(self, inode, ctx=None):
        info = Inode.by_inode(inode)
        return self._getattr(info, ctx)


    def _getattr(self, info, ctx):
        if info.inode() == pyfuse3.ROOT_INODE:
            is_dir = True
        else:
            is_dir = info.is_dir()

        entry = pyfuse3.EntryAttributes()
        entry.st_ino = info.inode()
        entry.generation = 0
        entry.entry_timeout = 300
        entry.attr_timeout = 300
        if is_dir:
            entry.st_mode = (stat.S_IFDIR | config.MODE_DIR)
        else:
            entry.st_mode = (stat.S_IFREG | config.MODE_FILE)

        entry.st_nlink = 1
        entry.st_uid = config.MOUNT_UID
        entry.st_gid = config.MOUNT_GID
        entry.st_rdev = 0
        entry.st_size = info.size()

        entry.st_blksize = 512 # TODO Use a sensible block size (same as chunk size?)
        entry.st_blocks = 1
        entry.st_atime_ns = 0
        entry.st_mtime_ns = info.mtime() * 1e6
        entry.st_ctime_ns = info.ctime() * 1e6

        return entry


    async def readlink(self, inode, ctx):
        raise(FUSEError(errno.ENOTSUP)) # Error: not supported


    async def opendir(self, fh, ctx):
        return fh


    async def readdir(self, fh, start_index, token):
        """
        Read entries in open directory fh.

        This method should list the contents of directory fh (as returned by a prior opendir call),
        starting at the entry identified by start_id.

        Instead of returning the directory entries directly, the method must call readdir_reply for
        each directory entry. If readdir_reply returns True, the file system must increase the lookup
        count for the provided directory entry by one and call readdir_reply again for the next entry
        (if any). If readdir_reply returns False, the lookup count must not be increased and the method
        should return without further calls to readdir_reply.

        The start_id parameter will be either zero (in which case listing should begin with the first
        entry) or it will correspond to a value that was previously passed by the file system to the
        readdir_reply function in the next_id parameter.

        If entries are added or removed during a readdir cycle, they may or may not be returned.
        However, they must not cause other entries to be skipped or returned more than once.

        . and .. entries may be included but are not required. However, if they are reported the
        filesystem must not increase the lookup count for the corresponding inodes (even if
        readdir_reply returns True).
        """

        info = Inode.by_inode(fh)
        entries = info.list_as_tuple()

        for i, (inode, name) in enumerate(entries):
            if i < start_index:
                continue
            if not pyfuse3.readdir_reply(token, name.encode(), await self.getattr(inode), i + 1):
                print('break')
                break


    async def unlink(self, inode_p, name,ctx):
        (success, response) = api.post('inodeDelete', data={'inode_p': inode_p, 'name': name})
        if not success:
            if response == 9:
                raise FUSEError(errno.EACCES) # Permission denied
            elif response in [22, 23, 25]:
                raise FUSEError(errno.ENOENT) # No such file or directory. but wait, what?? should not be possible
            else:
                print(response)
                raise(FUSEError(errno.EREMOTEIO)) # Remote I/O error


    async def rmdir(self, inode_p, name, ctx):
        (success, response) = api.post('inodeDelete', data={'inode_p': inode_p, 'name': name})
        if not success:
            if response == 10:
                raise FUSEError(errno.ENOTEMPTY) # Directory not empty
            elif response == 9:
                raise FUSEError(errno.EACCES) # Permission denied
            elif response in [22,23,25]:
                raise FUSEError(errno.ENOENT) # No such file or directory. but wait, what?? should not be possible
            else:
                print(response)
                raise(FUSEError(errno.EREMOTEIO)) # Remote I/O error


    async def symlink(self, inode_p, name, target, ctx):
        raise(FUSEError(errno.ENOTSUP)) # Error: not supported


    async def rename(self, inode_p_old, name_old, inode_p_new, name_new,
                     flags, ctx):
        """
        Rename a directory entry.

        This method must rename name_old in the directory with inode parent_inode_old to name_new in
        the directory with inode parent_inode_new. If name_new already exists, it should be overwritten.

        flags may be RENAME_EXCHANGE or RENAME_NOREPLACE. If RENAME_NOREPLACE is specified, the
        filesystem must not overwrite name_new if it exists and return an error instead. If
        RENAME_EXCHANGE is specified, the filesystem must atomically exchange the two files,
        i.e. both must exist and neither may be deleted.

        ctx will be a RequestContext instance.

        Let the inode associated with name_old in parent_inode_old be inode_moved, and the inode
         associated with name_new in parent_inode_new (if it exists) be called inode_deref.

        If inode_deref exists and has a non-zero lookup count, or if there are other directory
        entries referring to inode_deref), the file system must update only the directory entry for
        name_new to point to inode_moved instead of inode_deref. (Potential) removal of inode_deref
        (containing the previous contents of name_new) must be deferred to the forget method to be
        carried out when the lookup count reaches zero (and of course only if at that point there
        are no more directory entries associated with inode_deref either).
        """
        if flags != 0:
            raise FUSEError(errno.EINVAL)

        (success, response) = api.post('inodeMove', data={'inode_p': inode_p_old, 'name': name_old, 'new_parent': inode_p_new, 'new_name': name_new})
        if not success:
            if response == 1:
                raise(FUSEError(errno.ENOENT)) # No such file or directory
            elif response == 6:
                raise(FUSEError(errno.EEXIST)) # File exists
            elif response == 9:
                raise(FUSEError(errno.EACCES)) # Permission denied
            else:
                print(response)
                raise(FUSEError(errno.EREMOTEIO)) # Remote I/O error


    async def link(self, inode, new_inode_p, new_name, ctx):
        raise(FUSEError(errno.ENOTSUP))


    async def setattr(self, inode, attr, fields, fh, ctx):
        """
        Change attributes of inode

        fields will be an SetattrFields instance that specifies which attributes are
        to be updated. attr will be an EntryAttributes instance for inode that contains
        the new values for changed attributes, and undefined values for all other attributes.

        Most file systems will additionally set the st_ctime_ns attribute to the current
        time (to indicate that the inode metadata was changed).

        If the syscall that is being processed received a file descriptor argument (like
        e.g. ftruncate(2) or fchmod(2)), fh will be the file handle returned by the
        corresponding call to the open handler. If the syscall was path based (like e.g.
        truncate(2) or chmod(2)), fh will be None.

        ctx will be a RequestContext instance.

        The method should return an EntryAttributes instance (containing both the changed
        and unchanged values).
        """
        # raise(FUSEError(errno.ENOTSUP))
        print('Ignoring setattr')
        # raise(NotImplementedError('Setting attributes is not supported'))
        # if fields.update_size:
        #     data = self.get_row('SELECT data FROM inodes WHERE id=?', (inode,))[0]
        #     if data is None:
        #         data = b''
        #     if len(data) < attr.st_size:
        #         data = data + b'\0' * (attr.st_size - len(data))
        #     else:
        #         data = data[:attr.st_size]
        #     self.cursor.execute('UPDATE inodes SET data=?, size=? WHERE id=?',
        #                         (memoryview(data), attr.st_size, inode))
        # if fields.update_mode:
        #     self.cursor.execute('UPDATE inodes SET mode=? WHERE id=?',
        #                         (attr.st_mode, inode))

        # if fields.update_uid:
        #     self.cursor.execute('UPDATE inodes SET uid=? WHERE id=?',
        #                         (attr.st_uid, inode))

        # if fields.update_gid:
        #     self.cursor.execute('UPDATE inodes SET gid=? WHERE id=?',
        #                         (attr.st_gid, inode))

        # # if fields.update_atime:
        # #     self.cursor.execute('UPDATE inodes SET atime_ns=? WHERE id=?',
        # #                         (attr.st_atime_ns, inode))

        # if fields.update_mtime:
        #     self.cursor.execute('UPDATE inodes SET time=? WHERE id=?',
        #                         (attr.st_mtime_ns / 1e9, inode))

        # if fields.update_ctime:
        #     self.cursor.execute('UPDATE inodes SET time=? WHERE id=?',
        #                         (attr.st_ctime_ns / 1e9, inode))
        # # else:
        # #     self.cursor.execute('UPDATE inodes SET ctime_ns=? WHERE id=?',
        # #                         (int(time()*1e9), inode))

        return await self.getattr(inode)


    async def mknod(self, inode_p, name, mode, rdev, ctx):
        raise(NotImplementedError('don\'t even know what this does'))
        # return await self._create(inode_p, name, mode, ctx, rdev=rdev)


    async def mkdir(self, inode_p, name, _mode, ctx):
        info = Inode.by_mkdir(inode_p, name)
        return self._getattr(info, ctx)


    # async def statfs(self, ctx):
        # raise(NotImplementedError('statfs not yet supported'))
        # stat_ = pyfuse3.StatvfsData()

        # stat_.f_bsize = 512
        # stat_.f_frsize = 512

        # size = self.get_row('SELECT SUM(size) FROM inodes')[0]
        # stat_.f_blocks = size // stat_.f_frsize
        # stat_.f_bfree = max(size // stat_.f_frsize, 1024)
        # stat_.f_bavail = stat_.f_bfree

        # inodes = self.get_row('SELECT COUNT(id) FROM inodes')[0]
        # stat_.f_files = inodes
        # stat_.f_ffree = max(inodes , 100)
        # stat_.f_favail = stat_.f_ffree

        # return stat_


    async def open(self, inode, flags, ctx):
        """
        Open a inode inode with flags.

        ctx will be a RequestContext instance.

        flags will be a bitwise or of the open flags described in the open(2) manpage and defined in
        the os module (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC)

        This method must return a FileInfo instance. The FileInfo.fh field must contain an integer file
        handle, which will be passed to the read, write, flush, fsync and release methods to identify the
        open file. The FileInfo instance may also have relevant configuration attributes set; see the
        FileInfo documentation for more information.
        """
        # print('Opening files not implemented')
        # raise(FUSEError(errno.ENOSYS))
        # self.inode_open_count[inode] += 1
        print('open', inode, flags)

        # make sure the inode exists and is a file
        info = Inode.by_inode(inode)
        if info.is_dir():
            raise(FUSEError(errno.EISDIR)) # Error: Is a directory

        # Use inodes as a file handles
        return pyfuse3.FileInfo(fh=inode)


    async def access(self, inode, mode, ctx):
        print('access')
        return True


    async def create(self, inode_p, name, mode, flags, ctx):
        """
        Create a file with permissions mode and open it with flags

        ctx will be a RequestContext instance.

        The method must return a tuple of the form (fi, attr), where fi is a FileInfo instance
        handle like the one returned by open and attr is an EntryAttributes instance with the
        attributes of the newly created directory entry.

        (Successful) execution of this handler increases the lookup count for the returned inode by one.
        """
        info = Inode.by_mkfile(inode_p, name)
        return (pyfuse3.FileInfo(fh=info.inode()), self._getattr(info, ctx))


    def _download_chunk(self, inode, chunk_index):
        print('_download_chunk', inode, chunk_index)
        # file_name = self._get_name(inode)
        # dir_path = self._get_full_path(self._get_parent(inode))
        (success, response) = api.post('chunkTransfer', data={'file': inode, 'chunk': chunk_index, 'type': 'download'})
        if not success:
            return 'apierror', response
        download_url = response['url']
        checksum = response['checksum']
        node_response = api.get_requests_session().get(download_url)
        chunk_data = node_response.content
        print('download finished')
        if hashlib.md5(chunk_data).hexdigest() == checksum:
            print('checksum valid! size of downloaded data:', len(chunk_data))
            decrypted_data = self._get_cipher(inode, chunk_index).decrypt(chunk_data)
            return 'success', decrypted_data
        else:
            print('checksum invalid')
            return 'checksum', chunk_data


    async def read(self, fh, offset, length):
        start_chunk = offset // config.CHUNKSIZE
        end_chunk = (offset+length) // config.CHUNKSIZE
        print('read', 'handle', fh, 'offset', offset, 'length', length, 'chunk start', start_chunk, 'chunk end', end_chunk)

        chunks_data = b''
        for chunk_index in range(start_chunk, end_chunk + 1):
            try:
                chunk_data = self._get_row('SELECT data FROM write_buffer WHERE inode=? AND chunk_index=?', (fh, chunk_index))['data']
                print('read: found data in local buffer')
            except NoSuchRowError:
                print('Chunk data not in write buffer for chunk index', chunk_index)
                self.read_cache_lock.acquire()
                try:
                    self.cursor.execute('DELETE FROM read_cache WHERE time < ?', (int(time.time()) - 10,))
                    chunk_data = self._get_row('SELECT data FROM read_cache WHERE inode=? AND chunk_index=?', (fh, chunk_index))['data']
                    print('Chunk data found in read cache')
                    self.read_cache_lock.release()
                except NoSuchRowError:
                    print('Chunk data not in read cache for chunk index', chunk_index)
                    (status, chunk_data) = self._download_chunk(fh, chunk_index)
                    if status != 'success':
                        print('Download error:', status, len(chunk_data))
                        self.read_cache_lock.release()
                        raise(FUSEError(errno.EIO))

                    values = (fh, chunk_index, chunk_data, int(time.time()), chunk_data, int(time.time()))
                    self.cursor.execute('INSERT INTO read_cache (inode, chunk_index, data, time) VALUES (?,?,?,?) ON CONFLICT(inode, chunk_index) DO UPDATE SET data=?, time=?', values)
                    self.read_cache_lock.release()

            chunks_data += chunk_data

        data_offset = offset % config.CHUNKSIZE
        return chunks_data[data_offset:data_offset+length]


    async def write(self, fh, offset, buf):
        """
        Write buf into fh at off

        fh will by an integer filehandle returned by a prior open or create call.

        This method must return the number of bytes written. However, unless the file system
        has been mounted with the direct_io option, the file system must always write all
        the provided data (i.e., return len(buf)).
        """
        start_chunk = offset // config.CHUNKSIZE
        end_chunk = (offset+len(buf)) // config.CHUNKSIZE
        print('writing', 'handle', fh, 'offset', offset, 'len(buf)', len(buf), 'chunk start', start_chunk, 'chunk end', end_chunk)

        self.write_lock.acquire()

        chunks_data = b''
        for chunk_index in range(start_chunk, end_chunk + 1):
            self.read_cache_lock.acquire()
            self.cursor.execute('DELETE FROM read_cache WHERE inode=? AND chunk_index=?', (fh, chunk_index))
            self.read_cache_lock.release()

            try:
                chunk_data = self._get_row('SELECT data FROM write_buffer WHERE inode=? AND chunk_index=?', (fh, chunk_index))['data']
            except NoSuchRowError:
                # data not written before, try to download an existing chunk
                (status, response) = self._download_chunk(fh, chunk_index)
                if status == 'success':
                    chunk_data = response
                elif status == 'apierror' and response == 15:
                    # chunk does not exist
                    print(f'Chunk {chunk_index} does not exist, using empty byte array')
                    chunk_data = b''
                else:
                    print('Unknown download error', status, response)
                    self.write_lock.release()
                    raise(FUSEError(errno.EREMOTEIO))

            # pad chunk with zero bytes if it's not the last chunk, so chunks align properly
            if chunk_index != end_chunk:
                print('padding chunk_index', chunk_index, 'end_chunk', end_chunk, 'with', config.CHUNKSIZE - len(chunk_data), 'bytes - before length', len(chunk_data))
                chunk_data = chunk_data + bytearray(config.CHUNKSIZE - len(chunk_data))
                print('length after padding', len(chunk_data))

            chunks_data += chunk_data

        data_offset = offset % config.CHUNKSIZE
        print('going to write data now, data size before', len(chunks_data))
        chunks_data = chunks_data[:data_offset] + buf + chunks_data[data_offset+len(buf):]
        print('data size after', len(chunks_data))

        for chunk_index in range(start_chunk, end_chunk + 1):
            data = chunks_data[(chunk_index-start_chunk)*config.CHUNKSIZE:(chunk_index-start_chunk+1)*config.CHUNKSIZE]
            print('putting data in write buffer - chunk index', chunk_index, 'offset start', (chunk_index-start_chunk)*config.CHUNKSIZE, 'offset end', (chunk_index-start_chunk+1)*config.CHUNKSIZE, 'data len', len(data))
            timestamp = int(time.time())
            values = (fh, chunk_index, data, timestamp, data, timestamp)
            self.cursor.execute('INSERT INTO write_buffer (inode, chunk_index, data, last_update) VALUES (?,?,?,?) ON CONFLICT(inode, chunk_index) DO UPDATE SET data=?, last_update=?', values)

        self.write_lock.release()

        self._clear_write_buffer()

        return len(buf)


    async def fsync(self, fh, datasync):
        self._clear_write_buffer(force=True)


    async def release(self, fh):
        print('release', fh)
        self._clear_write_buffer(force=True)
        # print('Release not implemented')
        # raise(FUSEError(errno.ENOSYS))
        # self.inode_open_count[fh] -= 1

        # if self.inode_open_count[fh] == 0:
        #     del self.inode_open_count[fh]
        #     if (await self.getattr(fh)).st_nlink == 0:
        #         self.cursor.execute("DELETE FROM inodes WHERE id=?", (fh,))


class NoUniqueValueError(Exception):
    def __str__(self):
        return 'Query generated more than 1 result row'


class NoSuchRowError(Exception):
    def __str__(self):
        return 'Query produced 0 result rows'

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')

    return parser.parse_args()


def construct_encryption_key():
    # also serves as a connectivity check

    response = api.get('getEncryptionKey', {'inode': config.ROOT_INODE})
    if response is None or not response[0]:
        print('Connection error, exiting')
        exit(1)

    (_success, response) = response
    key = response['key'].encode()
    if len(key) != 32:
        print('Key must be 32 bytes long, it is', len(key), 'bytes.')
        exit(1)

    return key


if __name__ == '__main__':
    key = construct_encryption_key()

    options = parse_args()
    init_logging(options.debug)
    operations = Operations(key)

    if pyfuse3.ROOT_INODE != config.ROOT_INODE:
        raise(Exception(pyfuse3.ROOT_INODE + ' ' + config.ROOT_INODE))

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=eclipfs')
    fuse_options.add('allow_other')
    # fuse_options.discard('default_permissions')
    if options.debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(operations, options.mountpoint, fuse_options)

    try:
        log.debug('Entering main loop..')
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close(unmount=False)
        raise

    log.debug('Unmounting..')
    pyfuse3.close()
