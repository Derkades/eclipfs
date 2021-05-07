#!/usr/bin/env python3
import os
import sys

import pyfuse3
import errno
import stat
import time
import logging
from collections import defaultdict
from pyfuse3 import FUSEError # pylint: disable=no-name-in-module
from argparse import ArgumentParser
import trio

import base64

import threading
import schedule
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
        self.cache_lock = threading.Lock()
        self.fh_lock = threading.Lock()
        self.encryption_key = encryption_key
        self.file_handles = {}
        self.read_cache = {}
        self.write_buffer = {}
        self.readdir_cache = {}


    def _get_cipher(self, inode, chunk_index):
        key = self.encryption_key
        # The IV does not need to be secret or secure, as long as it is unique. Every chunk is guaranteed to have a
        # unique inode+chunk_index combination.
        # padding (4 bytes) + inode (8 bytes) + chunk_index (4 bytes) = 16 bytes IV
        iv = inode.to_bytes(12, byteorder='big') + chunk_index.to_bytes(4, byteorder='big')
        return AES.new(key, AES.MODE_CFB, iv)


    def _should_process_buffer(self, force):
        num_entries = len(self.write_buffer)
        log.debug('Write buffer entries: %s', num_entries)
        result = force and num_entries > 0 or not force and num_entries >= config.MAX_WRITE_BUFFER_SIZE
        log.debug('Should process buffer with %s entries force=%s? %s', num_entries, force, result)
        return result


    def _next_write_buffer_entry(self, force):
        if not self._should_process_buffer(force):
            return None

        return next(iter(self.write_buffer.keys()))


    def _clear_write_buffer(self, force=False):
        self.cache_lock.acquire()

        log.debug('Processing write buffer (force=%s)', force)

        key = self._next_write_buffer_entry(force)

        failure = True # so the loop runs the first time
        while failure:
            failure = False

            while key:
                (chunk_data, _last_update) = self.write_buffer[key]
                (inode, chunk_index) = key

                chunk_data_encrypted = self._get_cipher(inode, chunk_index).encrypt(chunk_data)
                chunk_data_checksum = hashlib.md5(chunk_data_encrypted).hexdigest()
                chunk_data_size = len(chunk_data_encrypted)

                assert len(chunk_data) == len(chunk_data_encrypted)

                log.debug('going to make chunkUploadInit request for inode %s chunk_index %s with size %s', inode, chunk_index, chunk_data_size)

                request_data = {
                    'file': inode,
                    'chunk': chunk_index,
                    'checksum': chunk_data_checksum,
                    'size': chunk_data_size
                }

                if config.PREFERRED_LOCATION:
                    request_data['location'] = config.PREFERRED_LOCATION

                (success, response) = api.post('chunkUploadInit', data=request_data)

                if success:
                    temp_id = response['id']
                    success_node_ids = []
                    nodes = response['nodes']
                    if len(nodes) == 0:
                        log.warning("Can't upload chunk, didn't receive any nodes from metaserver")
                        failure = True
                        break

                    log.info('Uploading chunk %s for inode %s', chunk_index, inode)

                    log.debug('chunkUploadInit request successful, uploading chunk to all chunkservers...')

                    for node in response['nodes']:
                        upload_address = node['address']
                        try:
                            r = requests.post(upload_address, data=chunk_data_encrypted, headers={'Content-Type':'application/octet-stream'})
                            if r.status_code == 200:
                                success_node_ids.append(node['id'])
                                log.info('Uploaded to node %s', node['id'])
                            else:
                                log.warn('Error during upload to node %s, http status code %s, response: %s', node, r.status_code, r.text)
                        except RequestException:
                            log.warn('Failed to connect to node %s', node)
                else: # chunkUploadInit not successful
                    if response == 2: # file not exists
                        log.warn('Failed to transfer chunk, file deleted while we were still uploading? Ignoring and removing from write buffer')
                        del self.write_buffer[key]
                        break
                    else:
                        log.warn('Unexpected chunkUploadInit failure, response: %s', response)
                        failure = True
                        break

                if len(success_node_ids) == 0:
                    log.warn("Can't upload chunk, metaserver didn't return any nodes for us to upload to")
                    failure = True
                    break

                log.debug('Chunk uploaded, going to make chunk upload finalize request')
                request_data = {
                    'id': temp_id,
                    'nodes': success_node_ids,
                }
                (success, response) = api.post('chunkUploadFinalize', data=request_data)
                if success:
                    log.debug('Finalized upload for chunk %s inode %s', chunk_index, inode)
                else:
                    if response == 2: # file not exists
                        log.warn('Failed to upload chunk, file no longer exists. Deleted in between upload and finalize? Ignoring and removing from write buffer')
                        del self.write_buffer[key]
                        break
                    else:
                        log.warn('Failure during chunkUploadFinalize %s', response)
                        failure = True
                        break

                # mark the chunk we just uploaded as not modified (functioning as read cache)
                log.debug('upload successful')

                # move from write buffer to read cache
                del self.write_buffer[key]
                self.read_cache[key] = (chunk_data, time.time())

                # get new chunk from write buffer for next iteration, if available
                key = self._next_write_buffer_entry(force)
            if failure:
                log.info('Errors were encountered while clearing the write buffer. Trying again in 5 seconds...')
                time.sleep(5)
            else:
                log.debug('Done clearing write buffer')

        self.cache_lock.release()


    def _obtain_file_handle(self, inode):
        inode_info = Inode.by_inode(inode)
        return self._obtain_file_handle_nofetch(inode_info), inode_info


    def _obtain_file_handle_nofetch(self, inode_info):
        self.fh_lock.acquire()
        fh = 0
        while fh in self.file_handles:
            fh += 1

        self.file_handles[fh] = inode_info
        self.fh_lock.release()
        log.debug('obtain file handle %s', fh)
        return fh


    def _release_file_handle(self, fh):
        log.debug('release file handle %s', fh)
        self.fh_lock.acquire()
        del self.file_handles[fh]
        if fh in self.readdir_cache:
            del self.readdir_cache[fh]
        self.fh_lock.release()


    def _update_file_handle(self, fh):
        log.debug('update file handle %s', fh)
        self.fh_lock.acquire()
        inode = self.file_handles[fh].inode()
        inode_info = Inode.by_inode(inode)
        self.file_handles[fh] = inode_info
        self.fh_lock.release()
        return inode_info


    def _get_fh_info(self, fh):
        self.fh_lock.acquire()
        info = self.file_handles[fh]
        self.fh_lock.release()
        return info


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

        entry.st_blksize = config.CHUNKSIZE
        entry.st_blocks = 1 if is_dir else info.chunks_count()
        entry.st_atime_ns = 0
        entry.st_mtime_ns = info.mtime() * 1e6
        entry.st_ctime_ns = info.ctime() * 1e6

        return entry


    async def readlink(self, inode, ctx):
        raise(FUSEError(errno.ENOTSUP)) # Error: not supported


    async def opendir(self, inode, ctx):
        (fh, _inode_info) = self._obtain_file_handle(inode)
        return fh


    async def releasedir(self, fh):
        self._release_file_handle(fh)


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
        self.fh_lock.acquire()
        if fh in self.readdir_cache:
            entries = self.readdir_cache[fh]
        else:
            info = self.file_handles[fh]
            entries = []
            entries.extend(info.list_dirs())
            entries.extend(info.list_files())
            self.readdir_cache[fh] = entries
        self.fh_lock.release()

        for i, (name, inode) in enumerate(entries):
            if i < start_index:
                continue
            if not pyfuse3.readdir_reply(token, name.encode(), await self.getattr(inode), i + 1):
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
        log.debug('delete done')


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
                log.warn('rmdir error, response: %s', response)
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
                log.warn('rename error, response: %s', response)
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
        log.warn('Ignoring setattr, not yet supported')
        # raise(FUSEError(errno.ENOTSUP))
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
        log.debug('open inode %s flags %s', inode, flags)

        # make sure the inode exists and is a file
        (fh, info) = self._obtain_file_handle(inode)
        if info.is_dir():
            self._release_file_handle(fh)
            raise(FUSEError(errno.EISDIR)) # Error: Is a directory

        return pyfuse3.FileInfo(fh=fh)


    async def access(self, inode, mode, ctx):
        log.debug('access')
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
        inode_info = Inode.by_mkfile(inode_p, name.decode())
        fh = self._obtain_file_handle_nofetch(inode_info)
        return (pyfuse3.FileInfo(fh=fh), self._getattr(inode_info, ctx))


    # def _download_chunk(self, inode, chunk_index):
    def _get_chunk_data(self, inode, chunk_index, tries=5):
        """
        LOCK CACHE WHEN USING THIS

        Returns:
            (None, chunk_data)
            ('apierror', error code)
            ('chunkservererror', http response)
            ('checksum', downloaded data)
        """

        # print('cache', int(time.time()) - config.READ_CACHE_TIME)
        # self.cursor.execute('SELECT inode,chunk_index,modified,last_update FROM chunk_cache')
        # for row in self.cursor:
            # print(row['inode'], row['chunk_index'], row['modified'], row['last_update'])

        # Try to find chunk in write buffer or read cache
        key = (inode, chunk_index)
        if key in self.write_buffer:
            (chunk_data, _last_update) = self.write_buffer[key]
            return chunk_data
        elif key in self.read_cache:
            (chunk_data, _last_update) = self.read_cache[key]
            return chunk_data
        else:
            # Chunk not found in cache, make request to metaserver to get a download url
            request_data = {
                'file': inode,
                'chunk': chunk_index
            }
            if config.PREFERRED_LOCATION:
                request_data['location'] = config.PREFERRED_LOCATION

            (success, response) = api.post('chunkDownload', data=request_data)
            if success:
                download_url = response['url']
                checksum = response['checksum']
                node_response = api.get_requests_session().get(download_url) # make request to chunkserver
                if node_response.status_code == 200:
                    chunk_data_encrypted = node_response.content
                    if hashlib.md5(chunk_data_encrypted).hexdigest() == checksum:
                        log.info('Downloaded chunk %s for inode %s', chunk_index, inode)
                        chunk_data = self._get_cipher(inode, chunk_index).decrypt(chunk_data_encrypted)

                        # insert downloaded data into read cache
                        self.read_cache[key] = (chunk_data, time.time())

                        return chunk_data
                    else:
                        log.info('Checksum error while downloading chunk, size of downloaded data was %s', len(chunk_data_encrypted))
                        if len(chunk_data_encrypted) < 300:
                            print('data:', chunk_data_encrypted)
                else:
                    log.warn('Chunk server non-200 HTTP response code while downloading data')
                    print(node_response.content.decode())
            else:
                if response == 15: # chunk not exists
                    log.debug(f'chunk {chunk_index} does not exist, returning empty byte array')
                    return b''
                else:
                    print('API error while downloading chunk', response)

            if tries == 0:
                log.error('Error during download on last try. Giving up and returning an error.')
                return None
            else:
                log.warn(f'Error during download, retrying ({tries} tries left).')
                return self._get_chunk_data(inode, chunk_index, tries=(tries - 1))


    async def read(self, fh, offset, length):
        start_chunk = offset // config.CHUNKSIZE
        end_chunk = (offset+length) // config.CHUNKSIZE
        inode_info = self._get_fh_info(fh)
        inode = inode_info.inode()

        log.debug('read fh %s, offset %s, len %s, start chunk %s, end chunk %s, inode %s, path %s', fh, offset, length, start_chunk, end_chunk, inode, inode_info.path())

        self.cache_lock.acquire() # _get_chunk_data() requires cache lock

        chunks_data = b''
        for chunk_index in range(start_chunk, end_chunk + 1):
            data = self._get_chunk_data(inode, chunk_index)

            if data is None:
                # Error during data download
                self.cache_lock.release()
                raise(FUSEError(errno.EREMOTEIO))

            # Data downloaded correctly
            chunks_data += data

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
        log.debug('write fh %s, offset %s, len %s, start chunk %s, end chunk %s', fh, offset, len(buf), start_chunk, end_chunk)

        inode_info = self._get_fh_info(fh)
        inode = inode_info.inode()

        self.cache_lock.acquire()

        chunks_data = b''
        for chunk_index in range(start_chunk, end_chunk + 1):

            chunk_data = self._get_chunk_data(inode, chunk_index)
            if chunk_data is None:
                self.cache_lock.release()
                raise(FUSEError(errno.EREMOTEIO))

            # pad chunk with zero bytes if it's not the last chunk, so chunks align properly
            if chunk_index != end_chunk:
                chunk_data = chunk_data + bytearray(config.CHUNKSIZE - len(chunk_data))

            chunks_data += chunk_data

        data_offset = offset % config.CHUNKSIZE
        chunks_data = chunks_data[:data_offset] + buf + chunks_data[data_offset+len(buf):]

        # write modified chunk data back to cache/write buffer

        for chunk_index in range(start_chunk, end_chunk + 1):
            chunk_data = chunks_data[(chunk_index-start_chunk)*config.CHUNKSIZE:(chunk_index-start_chunk+1)*config.CHUNKSIZE]

            # add to write cache and remove from read cache if present
            key = (inode, chunk_index)
            self.write_buffer[key] = (chunk_data, time.time())

            if key in self.read_cache:
                del self.read_cache[key]

        self.cache_lock.release()

        self._clear_write_buffer()

        return len(buf)


    async def fsync(self, fh, datasync):
        self._clear_write_buffer(force=True)


    async def release(self, fh):
        log.debug('release fh %s', fh)
        self._release_file_handle(fh)
        self._clear_write_buffer(force=True)


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

    response = api.get('getEncryptionKey')
    if response is None or not response[0]:
        log.error('Connection error, exiting')
        exit(1)

    (_success, response) = response
    key_encoded = response['key']

    log.info('Using encryption key (base64) %s', key_encoded)

    key = base64.b64decode(key_encoded)

    if len(key) != 32:
        log.error('Key must be 32 bytes long, it is %s bytes.', len(key))
        exit(1)

    return key


def clean_read_cache(operations):
    operations.cache_lock.acquire()

    log.debug('Read cache contains %s entries', len(operations.read_cache))
    to_remove = []
    for key in operations.read_cache.keys():
        (_chunk_data, last_update) = operations.read_cache[key]
        if last_update + 30 < time.time():
            log.debug('Removing %s from read cache', key)
            to_remove.append(key)

    for key in to_remove:
        del operations.read_cache[key]

    operations.cache_lock.release()


def timers(operations):
    schedule.every(5).to(10).seconds.do(lambda: clean_read_cache(operations))
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    options = parse_args()
    init_logging(options.debug)
    key = construct_encryption_key()

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

    t = threading.Thread(target=timers, args=[operations])
    t.daemon = True # required to exit nicely on SIGINT
    t.start()

    log.info('Started successfully')

    try:
        log.debug('Entering main loop..')
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close(unmount=False)
        raise

    log.debug('Unmounting..')
    pyfuse3.close()
