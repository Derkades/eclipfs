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
    enable_writeback_cache = config.ENABLE_WRITEBACK_CACHE

    def __init__(self, encryption_key):
        super(Operations, self).__init__()
        self.db = sqlite3.connect(':memory:')
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self.inode_open_count = defaultdict(int)
        self.init_tables()
        self.cache_lock = threading.Lock()
        self.encryption_key = encryption_key
        self.dir_handles = {}


    def init_tables(self):
        self.cursor.execute("""
        CREATE TABLE chunk_cache (
            inode           BIGINT NOT NULL,
            chunk_index     INTEGER NOT NULL,
            data            bytea NOT NULL,
            last_update     BIGINT NOT NULL,
            modified        BOOLEAN NOT NULL,
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
        if force:
            print('force clear write buffer')

        self.cache_lock.acquire()

        num_entries = self._get_row("SELECT COUNT(*) FROM chunk_cache WHERE modified = 'True'")[0]
        # print('Write buffer contains', num_entries, 'entries')
        if force and num_entries == 0 or not force and num_entries < 5:
            self.cache_lock.release()
            return

        try:
            row = self._get_row("SELECT inode,chunk_index,data FROM chunk_cache WHERE modified = 'True' LIMIT 1")
        except NoSuchRowError:
            # this should never happen, right?
            raise(Exception("this is bad"))

        failure = True # so the loop runs the first time
        while failure:
            failure = False

            while row:
                (inode, chunk_index, chunk_data) = row

                chunk_data_encrypted = self._get_cipher(inode, chunk_index).encrypt(chunk_data)
                chunk_data_checksum = hashlib.md5(chunk_data_encrypted).hexdigest()
                chunk_data_size = len(chunk_data_encrypted)

                assert len(chunk_data) == len(chunk_data_encrypted)

                print('clearing write buffer, making chunkTransfer request', inode, chunk_index, chunk_data_checksum, chunk_data_size, inode)

                request_data = {
                    'file': inode,
                    'chunk': chunk_index,
                    'type': 'upload',
                    'checksum': chunk_data_checksum,
                    'size': chunk_data_size
                }

                if config.PREFERRED_LOCATION:
                    request_data['location'] = config.PREFERRED_LOCATION

                (success, response) = api.post('chunkTransfer', data=request_data)

                if success:
                    url = response['url']
                    try:
                        r = requests.post(url, data=chunk_data_encrypted, headers={'Content-Type':'application/octet-stream'})
                    except RequestException:
                        print('Failed to connect to node')
                        failure = True
                        break

                    if r.status_code != 200:
                        print('FAILED TO UPLOAD CHUNK status code', r.status_code)
                        failure = True
                        break

                else: # not success
                    if response == 2: # file not exists
                        print('Failed to transfer chunk, file no longer exists. Ignoring.')
                    else:
                        print('FAILED TO REQUEST CHUNK TRANSFER', inode, chunk_index)
                        failure = True
                        break

                # mark the chunk we just uploaded as not modified (functioning as read cache)
                print('upload successful', chunk_data_size)
                self.cursor.execute("UPDATE chunk_cache SET modified = 'False' WHERE inode=? AND chunk_index=?", (inode, chunk_index))

                try:
                    row = self._get_row("SELECT inode,chunk_index,data FROM chunk_cache WHERE modified = 'True' LIMIT 1")
                    # print('Not done yet, there are more entries in the write buffer...')
                except NoSuchRowError:
                    row = None

            if failure:
                print('Errors were encountered while clearing the write buffer. Trying again in 5 seconds...')
                time.sleep(5)
            else:
                print('done clearing write buffer')

        self.cache_lock.release()


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
            info = Inode.by_name(inode_p, name.decode())
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


    async def opendir(self, inode, ctx):
        fh = 0
        while fh in self.dir_handles:
            fh += 1

        self.dir_handles[fh] = Inode.by_inode(inode)
        print('opendir', inode, fh)
        return fh


    async def releasedir(self, fh):
        del self.dir_handles[fh]


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
        info = self.dir_handles[fh]
        entries = info.list_as_tuple()

        for i, (inode, name) in enumerate(entries):
            if i < start_index:
                continue
            if not pyfuse3.readdir_reply(token, name.encode(), await self.getattr(inode), i + 1):
                # print('break')
                break


    async def unlink(self, inode_p, name,ctx):
        (success, response) = api.post('inodeDelete', data={'inode_p': inode_p, 'name': name.decode()})
        if not success:
            if response == 9:
                raise FUSEError(errno.EACCES) # Permission denied
            elif response in [22, 23, 25]:
                raise FUSEError(errno.ENOENT) # No such file or directory. but wait, what?? should not be possible
            else:
                print('unlink error', response)
                raise(FUSEError(errno.EREMOTEIO)) # Remote I/O error
        print('delete done')


    async def rmdir(self, inode_p, name, ctx):
        (success, response) = api.post('inodeDelete', data={'inode_p': inode_p, 'name': name.decode()})
        if not success:
            if response == 10:
                raise FUSEError(errno.ENOTEMPTY) # Directory not empty
            elif response == 9:
                raise FUSEError(errno.EACCES) # Permission denied
            elif response in [22,23,25]:
                raise FUSEError(errno.ENOENT) # No such file or directory. but wait, what?? should not be possible
            else:
                print('rmdir error', response)
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

        (success, response) = api.post('inodeMove', data={'inode_p': inode_p_old, 'name': name_old.decode(), 'new_parent': inode_p_new, 'new_name': name_new.decode()})
        if not success:
            if response == 1:
                raise(FUSEError(errno.ENOENT)) # No such file or directory
            elif response == 6:
                raise(FUSEError(errno.EEXIST)) # File exists
            elif response == 9:
                raise(FUSEError(errno.EACCES)) # Permission denied
            else:
                print('rename error', response)
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
        info = Inode.by_mkdir(inode_p, name.decode())
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
        info = Inode.by_mkfile(inode_p, name.decode())
        return (pyfuse3.FileInfo(fh=info.inode()), self._getattr(info, ctx))


    # def _download_chunk(self, inode, chunk_index):
    def _get_chunk_data(self, inode, chunk_index):
        """
        LOCK CACHE WHEN USING THIS

        Returns:
            (None, chunk_data)
            ('apierror', error code)
            ('checksum', length of downloaded data)
        """
        # print('_download_chunk', inode, chunk_index)

        # Remove outdated entries from chunk cache. Only remove non-modified entries, modified entries still need to be uploaded!
        self.cursor.execute("DELETE FROM chunk_cache WHERE modified = 'False' AND last_update < ?", (int(time.time()) - config.READ_CACHE_TIME,))

        try:
            # Try to find chunk in read cache
            chunk_data = self._get_row('SELECT data FROM chunk_cache WHERE inode=? AND chunk_index=?', (inode, chunk_index))['data']
            # print('Chunk data found in cache')
            # self.read_cache_lock.release()
            return None, chunk_data
        except NoSuchRowError:
            print('Chunk data not in cache for chunk index, need to download.', chunk_index)
            request_data = {
                'file': inode,
                'chunk': chunk_index,
                'type': 'download'
            }
            if config.PREFERRED_LOCATION:
                request_data['location'] = config.PREFERRED_LOCATION

            (success, response) = api.post('chunkTransfer', data=request_data)
            if not success:
                return 'apierror', response

            download_url = response['url']
            checksum = response['checksum']
            node_response = api.get_requests_session().get(download_url) # make request to node
            chunk_data_encrypted = node_response.content
            print('download finished')
            if hashlib.md5(chunk_data_encrypted).hexdigest() == checksum:
                print('checksum valid! size of downloaded data:', len(chunk_data_encrypted))
                chunk_data = self._get_cipher(inode, chunk_index).decrypt(chunk_data_encrypted)

                # there should never be a conflict here, if there was we would've returned data from the read cache instead of downloading it
                self.cursor.execute("INSERT INTO chunk_cache (inode, chunk_index, data, last_update, modified) VALUES (?,?,?,?,'False')", (inode, chunk_index, chunk_data, int(time.time())))
                return None, chunk_data
            else:
                print('checksum invalid')
                return 'checksum', len(chunk_data_encrypted)


    async def read(self, fh, offset, length):
        start_chunk = offset // config.CHUNKSIZE
        end_chunk = (offset+length) // config.CHUNKSIZE
        print('read -', 'handle', fh, 'offset', offset, 'length', length, 'chunk start', start_chunk, 'chunk end', end_chunk)

        self.cache_lock.acquire()

        chunks_data = b''
        for chunk_index in range(start_chunk, end_chunk + 1):
            (error, data) = self._get_chunk_data(fh, chunk_index)
            if not error:
                chunks_data += data
                continue

            if error == 'apierror':
                print('API error while downloading chunk', data)
                self.cache_lock.release()
                raise(FUSEError(errno.EREMOTEIO))
            elif error == 'checksum':
                print('Checksum error while downloading chunk, size of downloaded data was', data)
                self.cache_lock.release()
                raise(FUSEError(errno.EREMOTEIO))
            else:
                raise(Exception('Unknown error while downloading'))

        self.cache_lock.release()

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
        print('write -', 'handle', fh, 'offset', offset, 'len(buf)', len(buf), 'chunk start', start_chunk, 'chunk end', end_chunk)

        self.cache_lock.acquire()

        chunks_data = b''
        for chunk_index in range(start_chunk, end_chunk + 1):

            (error, data) = self._get_chunk_data(fh, chunk_index)
            if error:
                if error == 'apierror':
                    if data == 15: # chunk not exists
                        print(f'chunk {chunk_index} does not exist, using empty byte array')
                        chunk_data = b''
                    else:
                        print('API error while downloading chunk', data)
                        raise(FUSEError(errno.EREMOTEIO))
                elif error == 'checksum':
                    print('Checksum error while downloading chunk, size of downloaded data was', data)
                    raise(FUSEError(errno.EREMOTEIO))
                else:
                    raise(Exception('Unknown error while downloading'))
            else:
                chunk_data = data

            # pad chunk with zero bytes if it's not the last chunk, so chunks align properly
            if chunk_index != end_chunk:
                chunk_data = chunk_data + bytearray(config.CHUNKSIZE - len(chunk_data))

            chunks_data += chunk_data

        data_offset = offset % config.CHUNKSIZE
        chunks_data = chunks_data[:data_offset] + buf + chunks_data[data_offset+len(buf):]

        # write modified chunk data back to cache/write buffer

        for chunk_index in range(start_chunk, end_chunk + 1):
            chunk_data = chunks_data[(chunk_index-start_chunk)*config.CHUNKSIZE:(chunk_index-start_chunk+1)*config.CHUNKSIZE]
            # print('put data in cache after write -', 'inode', fh, 'chunk_index', chunk_index, 'len(chunk_data)', len(chunk_data))
            # set modified = True so the write buffer clear function knows it needs to upload it
            query = "INSERT INTO chunk_cache (inode, chunk_index, data, last_update, modified) VALUES (?,?,?,?,'True') ON CONFLICT(inode, chunk_index) DO UPDATE SET data=?, last_update=?, modified='True'"
            self.cursor.execute(query, (fh, chunk_index, chunk_data, int(time.time()), chunk_data, int(time.time())))

        self.cache_lock.release()

        self._clear_write_buffer()

        return len(buf)


    async def fsync(self, fh, datasync):
        self._clear_write_buffer(force=True)


    async def release(self, fh):
        print('release', fh)
        self._clear_write_buffer(force=True)
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

    print('Successfully connected to metaserver')

    options = parse_args()
    init_logging(options.debug)
    operations = Operations(key)

    # os.mkdir(options.mountpoint)

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
