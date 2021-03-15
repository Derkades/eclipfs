#!/usr/bin/env python3
import os
import sys

import pyfuse3
import errno
import stat
from time import time
import sqlite3
import logging
from collections import defaultdict
from pyfuse3 import FUSEError
from argparse import ArgumentParser
import trio

import threading
import config
import api

import requests
import hashlib
import time

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

log = logging.getLogger()

class Operations(pyfuse3.Operations):
    supports_dot_lookup = True
    enable_writeback_cache = False

    def __init__(self):
        super(Operations, self).__init__()
        self.db = sqlite3.connect(':memory:')
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self.inode_open_count = defaultdict(int)
        self.init_tables()
        self.read_lock = threading.Lock()
        self.write_lock = threading.Lock()

    def init_tables(self):
        # self.cursor.execute("""
        # CREATE TABLE inode_map (
        #     inode           INTEGER PRIMARY KEY,
        #     inode_p         INTEGER NOT NULL,
        #     name            TEXT NOT NULL,
        #     is_dir          BOOLEAN NOT NULL,
        #     size            LONG NOT NULL,
        #     UNIQUE(inode_p, name)
        # )
        # """)

        # Skip inode=1, this is already used by the root dir
        self.cursor.execute("INSERT INTO inode_map VALUES (1, 0, 'ROOT_DIRECTORY', 1, 0)")

        self.cursor.execute("""
        CREATE TABLE write_buffer (
            inode           INTEGER NOT NULL,
            chunk_index     INTEGER NOT NULL,
            data            bytea NOT NULL,
            last_update     BIGINT NOT NULL,
            PRIMARY KEY(inode, chunk_index),
            UNIQUE(inode, chunk_index)
        )
        """)

        # self.cursor.execute("""
        # CREATE TABLE read_cache (
        #     inode           INTEGER NOT NULL,
        #     directory       TEXT NOT NULL,
        #     filename        TEXT NOT NULL,
        #     chunk_index     INTEGER NOT NULL,
        #     data            bytea NOT NULL,
        #     UNIQUE(directory, filename, chunk_index),
        #     UNIQUE(inode, chunk_index)
        # )
        # """)

        # self.cursor.execute("""
        # CREATE TABLE write_cache (
        #     inode           INTEGER NOT NULL,
        #     directory       TEXT NOT NULL,
        #     filename        TEXT NOT NULL,
        #     chunk_index     INTEGER NOT NULL,
        #     data            bytea NOT NULL,
        #     UNIQUE(directory, filename, chunk_index),
        #     UNIQUE(inode, chunk_index)
        # )
        # """)


    def _get_full_path(self, inode):
        """
        Get absolute path. Needs to go up the tree recursively.
        """
        self._print_db()
        print('_get_full_path', inode)

        if inode == pyfuse3.ROOT_INODE:
            return '/'
        else:
            full_path = self._get_name(inode)
            inode_p = self._get_parent(inode)
            while inode_p:
                full_path = self._get_name(inode_p) + '/' + full_path
                inode_p = self._get_parent(inode_p)

            if not full_path.startswith("ROOT_DIRECTORY"):
                raise Exception('Full path does not start with ROOT_DIRECTORY', full_path)

            return full_path[14:] # remove ROOT_DIRECTORY (but leave / after it)


    def _get_parent(self, inode):
        """
        Get inode of parent directory for the specified file or directory inode. Throws an exception if the inode is invalid, returns None if it has no parent.
        """
        if inode == pyfuse3.ROOT_INODE:
            return None

        return self._get_row("SELECT inode_p FROM inode_map WHERE inode=?", (inode,))['inode_p']


    def _get_name(self, inode):
        """
        Get file name by inode
        """
        return self._get_row("SELECT name FROM inode_map WHERE inode=?", (inode,))['name']


    def _get_type(self, inode):
        """
        Get inode type

        Parameters:
            inode

        Returns:
            'f' for files, 'd' for directories, 'None' if the inode does not exist.
        """
        try:
            is_dir = self._get_row('SELECT is_dir FROM inode_map WHERE inode=?', (inode,))['is_dir']
            return 'd' if is_dir else 'f'
        except NoSuchRowError:
            return None


    def _print_db(self):
        """
        Print inode map for debugging
        """
        self.cursor.execute("SELECT * FROM inode_map")
        for row in self.cursor:
            print('inode', row['inode'], 'inode_p', row['inode_p'], 'name', row['name'], 'is_dir', row['is_dir'])


    def _get_inode(self, inode_p, name):
        print('_get_inode', inode_p, name)
        """
        Get inode for a file or directory

        Parameters:
            inode_p: Parent directory inode
            name: Name of new or existing file or directory

        Returns:
            Inode of created file or directory

        Throws:
            NoSuchRowError if no such entry exists
        """
        if type(name) == bytes:
            name = name.decode()

        return self._get_row('SELECT inode FROM inode_map WHERE inode_p=? AND name=?', (inode_p, name))['inode']


    def _create_inode(self, inode_p, name, i_type, size):
        """
        Get inode for a file or directory, creating one if it does not exist yet.

        Parameters:
            inode_p: Parent directory inode
            name: Name of new or existing file or directory
            i_type: Inode type, 'f' for files, 'd' for directories.

        Returns:
            Inode of created file or directory
        """
        if type(name) == bytes:
            name = name.decode()

        try:
            return self._get_inode(inode_p, name)
        except NoSuchRowError:
            is_dir = i_type == 'd'
            self.cursor.execute("INSERT INTO inode_map (inode_p, name, is_dir, size) VALUES (?, ?, ?, ?)", (inode_p, name, is_dir, size))
            return self.cursor.lastrowid


    def _inode_ls_inode(self, inode_p):
        """
        List files and directories in a directory

        Parameters:
            inode_p: Inode of parent directory

        Returns:
            List of inodes
        """
        self.cursor.execute("SELECT inode FROM inode_map WHERE inode_p=?", (inode_p,))
        return [row['inode'] for row in self.cursor]


    def _inode_ls_name(self, inode_p):
        """
        List files and directories in a directory

        Parameters:
            inode_p: Inode of parent directory

        Returns:
            List of file/directory names
        """
        self.cursor.execute("SELECT name FROM inode_map WHERE inode_p=?", (inode_p,))
        return [row['name'] for row in self.cursor]


    def _inode_ls(self, inode_p):
        """
        List files and directories in a directory

        Parameters:
            inode_p: Inode of parent directory

        Returns:
            List of tuples: (inode, file/directory name, type ('f'/'d')
        """
        self.cursor.execute("SELECT inode,name,is_dir FROM inode_map WHERE inode_p=?", (inode_p,))
        return [(row['inode'], row['name'], 'd' if row['is_dir'] else 'f') for row in self.cursor]


    def _get_size(self, inode):
        return self._get_row('SELECT size FROM inode_map WHERE inode=?', (inode,))['size']


    def _refresh_dir(self, inode_p):
        """
        Refresh local inode map for the specified inode. Adds any new directories and removes any deleted directories. Files are not handled yet.

        Parameters:
            inode_p
        """
        # self.cursor.execute('DELETE FROM inode_map WHERE inode_p=?', (inode_p,))
        # self.cursor.execute("SELECT name FROM inode_map WHERE inode_p=?", (inode_p,))
        # old_subdir_names = {row['path'] for row in self.cursor}
        old_entries = self._inode_ls(inode_p)
        old_dir_names = []
        old_file_names = []

        for old_entry in old_entries:
            (_inode, name, i_type) = old_entry
            if i_type == 'f':
                old_file_names.append(name)
            elif i_type == 'd':
                old_dir_names.append(name)
            else:
                raise Exception()

        old_dir_names = set(old_dir_names)
        old_file_names = set(old_file_names)

        if inode_p == pyfuse3.ROOT_INODE:
            (_success, response) = api.get('directoryListRoot')
            directories = response['directories']
            new_dir_names = {directory['name'] for directory in directories}
            new_file_names_sizes = {} # can't have files in root directory
        else:
            path_p = self._get_full_path(inode_p)
            (_success, response) = api.get('directoryInfo', params={'path': path_p})
            # TODO handle api errors
            new_dir_names = set(response['directory']['directories'])
            new_file_names_sizes = {}
            for file_obj in response['directory']['files']:
                # print(file_obj)
                new_file_names_sizes[file_obj['name']] = file_obj['size']

        for old_dir_name in old_dir_names:
            if old_dir_name not in new_dir_names:
                # a directory has been removed on the remote
                print('removing local directory', old_dir_name, old_dir_names, new_dir_names)
                self.cursor.execute('DELETE FROM inode_map WHERE inode_p=? AND name=?', (inode_p, old_dir_name))

        for old_file_name in old_file_names:
            if old_file_name not in new_file_names_sizes:
                # a file has been removed on the remote
                print('removing local file', old_file_name, old_file_names, new_file_names_sizes)
                self.cursor.execute('DELETE FROM inode_map WHERE inode_p=? AND name=?', (inode_p, old_file_name))

        for new_dir_name in new_dir_names:
            if new_dir_name not in old_dir_names:
                # a new directory has been added on the remote
                self._create_inode(inode_p, new_dir_name, 'd', size=0)

        for new_file_name in new_file_names_sizes:
            if new_file_name not in old_file_names:
                # a new file has been added on the remote
                size = new_file_names_sizes[new_file_name]
                self._create_inode(inode_p, new_file_name, 'f', size)

        # Update file sizes
        for new_file_name in new_file_names_sizes:
            size = new_file_names_sizes[new_file_name]
            # print(size)
            self.cursor.execute('UPDATE inode_map SET size=? WHERE name=? AND inode_p=?', (size, new_file_name, inode_p))


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


    def _upload_chunk(self):
        print('upload chunk')


    def _clear_write_buffer(self, time_before=None):
        print('clear write buffer')
        #         CREATE TABLE write_buffer (
        #     inode           INTEGER NOT NULL,
        #     chunk_index     INTEGER NOT NULL,
        #     data            bytea NOT NULL,
        #     last_update     BIGINT NOT NULL,
        #     PRIMARY KEY(inode, chunk_index),
        #     UNIQUE(inode, chunk_index)
        # )
        self.write_lock.acquire()
        try:
            inode = self._get_row('SELECT inode FROM write_buffer LIMIT 1')['inode']
        except NoSuchRowError:
            print('Write queue is empty')
            inode = None

        while inode:
            # Note: one inode in the write buffer may appear multiple times, with different chunk indices. We only selet one
            (data, chunk_index) = self._get_row('SELECT data,chunk_index FROM write_buffer WHERE inode=? LIMIT 1', (inode,))
            inode_p = self._get_parent(inode)
            dir_path = self._get_full_path(inode_p)
            file_name = self._get_name(inode)

            checksum = hashlib.md5(data).hexdigest()
            size = len(data)

            print('making chunkTransfer request', inode, chunk_index, inode_p, dir_path, file_name, checksum, size)

            (success, response) = api.post('chunkTransfer', data={'directory': dir_path, 'file': file_name, 'chunk': chunk_index, 'type': 'upload', 'checksum': checksum, 'size': size})

            if not success:
                print('FAILED TO REQUEST CHUNK TRANSFER', inode, chunk_index)
                self.write_lock.release()
                raise(FUSEError(errno.EREMOTEIO))

            url = response['url']

            r = requests.post(url, data=data, headers={'Content-Type':'application/octet-stream'})

            if r.status_code != 200:
                print('FAILED TO UPLOAD CHUNK status code', r.status_code, r.content)
                self.write_lock.release()
                raise(FUSEError(errno.EREMOTEIO))

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
            self._refresh_dir(inode_p)
            try:
                inode = self._get_inode(inode_p, name)
            except NoSuchRowError:
                raise(pyfuse3.FUSEError(errno.ENOENT))

        return await self.getattr(inode, ctx)


    async def getattr(self, inode, ctx=None):
        # print(f'getattr inode={inode}')
        if inode == pyfuse3.ROOT_INODE:
            is_dir = True
        else:
            is_dir = self._get_type(inode) == 'd'

        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
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
        entry.st_size = self._get_size(inode)

        entry.st_blksize = 512 # TODO Use a sensible block size (same as chunk size?)
        entry.st_blocks = 1
        # time_ns = row['time'] * 1e9
        # TODO Time
        entry.st_atime_ns = 0
        entry.st_mtime_ns = 0
        entry.st_ctime_ns = 0

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
        inode_p = fh

        if start_index == 0:
            self._refresh_dir(inode_p)
            self._print_db()

        # TODO more efficient approach, select necessary rows only (starting at start_index)
        self.cursor.execute("SELECT inode,name FROM inode_map WHERE inode_p=?", (inode_p,))

        for i, row in enumerate(self.cursor):
            if i < start_index:
                continue
            name = row['name']
            inode = row['inode']
            if not pyfuse3.readdir_reply(token, name.encode(), await self.getattr(inode), i + 1):
                print('break')
                break


    async def unlink(self, inode_p, name,ctx):
        raise(FUSEError(errno.ENOTSUP)) # Error: not supported


    async def rmdir(self, inode_p, name, ctx):
        # build local tree for parent directory, it may be msising if user hasn't done ls yet
        # self.refresh_dir_inode_map(inode_p)
        self._refresh_dir(inode_p)
        try:
            inode = self._get_inode(inode_p, name)
        except NoSuchRowError:
            raise FUSEError(errno.ENOENT) # No such file or directory

        path = self._get_full_path(inode)
        (success, response) = api.post('directoryDelete', data={'path': path})
        if not success:
            if response == 10:
                raise FUSEError(errno.ENOTEMPTY) # Directory not empty
            elif response == 9:
                raise FUSEError(errno.EACCES) # Permission denied
            elif response == 1:
                raise FUSEError(errno.ENOENT) # No such file or directory. but wait, what?? should not be possible
            else:
                raise(FUSEError(errno.EREMOTEIO)) # Remote I/O error

        # refresh again to delete directory locally if it was deleted remotely
        self._refresh_dir(inode_p)
        # self.cursor.execute("DELETE FROM inode_map WHERE inode_p=? AND name=?", (inode_p, name))


    async def symlink(self, inode_p, name, target, ctx):
        raise(FUSEError(errno.ENOTSUP)) # Error: not supported


    # def _update_paths_rename(self, inode_top, path_old, path_new):
    #     self.cursor.execute('SELECT inode,path FROM inode_map WHERE inode_p=?', (inode_top,))
    #     for row in self.cursor:
    #         subdir_inode = row['inode']
    #         subdir_path = row['path']
    #         subdir_path_new = subdir_path.replace(path_old, path_new, 1)
    #         subdir_name_new = subdir_path_new.split('/')[-1]
    #         print('updating path from', subdir_path, 'to', subdir_path_new)
    #         self.cursor.execute('UPDATE inode_map SET path=?, name=? WHERE inode=?', (subdir_path_new, subdir_name_new, subdir_inode))
    #         self._update_paths_rename(subdir_inode, path_old, path_new)


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

        # as always, refresh local cache before any operation
        self._refresh_dir(inode_p_old)
        if inode_p_new != inode_p_old:
            self._refresh_dir(inode_p_new)

        inode = self._get_inode(inode_p_old, name_old)

        path_old = self._get_full_path(inode)
        path_new_a = self._get_full_path(inode_p_new)
        path_new_b = name_new.decode()
        if not path_new_b.startswith('/') and not path_new_a.endswith('/'):
            path_new_a += '/'
        path_new = path_new_a + path_new_b

        print('paths', path_old, path_new)

        i_type = self._get_type(inode)

        if i_type == 'd':
            (success, response) = api.post('directoryMove', data={'path_old': path_old, 'path_new': path_new})
            if success:
                print('success')

                if name_new.decode() != path_new.split('/')[-1]:
                    raise(Exception('New name is not same as last part of new path?', name_new, path_new))

                # Update name for moved directory
                self.cursor.execute('UPDATE inode_map SET name=? WHERE inode=?', (name_new, inode))
                # Update directory listing for old parent directory
                self._refresh_dir(inode_p_old)
                # Update directory listing for new parent directory
                self._refresh_dir(inode_p_new)
            else:
                if response == 1:
                    raise(FUSEError(errno.ENOENT)) # No such file or directory
                elif response == 6:
                    raise(FUSEError(errno.EEXIST)) # File exists
                elif response == 9:
                    raise(FUSEError(errno.EACCES)) # Permission denied
                else:
                    raise(FUSEError(errno.EREMOTEIO)) # Remote I/O error
        elif i_type == 'f':
            # Moving files not implemented yet
            raise(FUSEError(errno.ENOSYS))
        else: # entry_type returns None if the file does not exist
            print('returned none')
            raise(FUSEError(errno.ENOENT))


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


    async def mkdir(self, inode_p, name, _mode, _ctx):
        # build local tree for parent directory, it may be msising if user hasn't done ls yet
        self._refresh_dir(inode_p)

        payload = {'name': name.decode()}
        if inode_p != pyfuse3.ROOT_INODE:
            parent_path = self._get_full_path(inode_p)
            payload['parent'] = parent_path

        (success, _response) = api.post('directoryCreate', payload)
        if not success:
            raise(Exception('Error creating directory'))

        # refresh directory again to add the new directory to local cache
        self._refresh_dir(inode_p)

        inode = self._get_inode(inode_p, name)
        return await self.getattr(inode)

        # return await self._create(inode_p, name, mode, ctx)


    async def statfs(self, ctx):
        raise(NotImplementedError('statfs not yet supported'))
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

        self._print_db()
        # make sure the inode exists and is a file
        i_type = self._get_type(inode)
        if i_type == 'f':
            pass  # good!
        elif i_type == 'd':
            raise(FUSEError(errno.EISDIR)) # Error: Is a directory
        else:
            raise(FUSEError(errno.ENOENT)) # Error: No such file or directory

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
        # print('Creating files not implemented')
        print('create', 'inode_p', inode_p, 'name', name, 'mode', mode, 'flags', flags)

        if inode_p == pyfuse3.ROOT_INODE:
            print('Cannot create file in root directory!')
            raise(FUSEError(errno.ENOTSUP))

        # as always, refresh before any operation
        self._refresh_dir(inode_p)

        dir_path = self._get_full_path(inode_p)

        # check if file exists
        try:
            inode = self._get_inode(inode_p, name)
            # no exception, it does!
            raise(FUSEError(errno.EEXIST)) # File exists
        except NoSuchRowError:
            # file does not exist, good
            pass

        (success, _response) = api.post('fileCreate', {'dir': dir_path, 'name': name})
        if not success:
            raise(FUSEError(errno.EREMOTEIO)) # Remote I/O error

        # refresh dir to get newly created file
        self._refresh_dir(inode_p)

        inode = self._get_inode(inode_p, name)

        return (pyfuse3.FileInfo(fh=inode), await self.getattr(inode))

        # raise(FUSEError(errno.ENOSYS))

        # entry = await self._create(inode_parent, name, mode, ctx)
        # self.inode_open_count[entry.st_ino] += 1
        # return (pyfuse3.FileInfo(fh=entry.st_ino), entry)


    def _download_chunk(self, inode, chunk_index):
        print('_download_chunk', inode, chunk_index)
        file_name = self._get_name(inode)
        dir_path = self._get_full_path(self._get_parent(inode))
        (success, response) = api.post('chunkTransfer', data={'directory': dir_path, 'file': file_name, 'chunk': chunk_index, 'type': 'download'})
        if not success:
            return 'apierror', response
        download_url = response['url']
        checksum = response['checksum']
        node_response = requests.get(download_url)
        chunk_data = node_response.content
        print('download finished')
        if hashlib.md5(chunk_data).hexdigest() == checksum:
            print('checksum valid! size of downloaded data:', len(chunk_data))
            return 'success', chunk_data
        else:
            print('checksum invalid', hashlib.md5(chunk_data).hexdigest(), checksum, len(chunk_data))
            return 'checksum', chunk_data


    async def read(self, fh, offset, length):
        start_chunk = offset // config.CHUNKSIZE
        end_chunk = (offset+length) // config.CHUNKSIZE
        inode = fh
        print('read', 'handle', fh, 'offset', offset, 'length', length, 'chunk start', start_chunk, 'chunk end', end_chunk)

        chunks_data = b''
        for chunk_index in range(start_chunk, end_chunk + 1):
            try:
                chunk_data = self._get_row('SELECT data FROM write_buffer WHERE inode=? AND chunk_index=?', (inode, chunk_index))['data']
                print('read: found data in local buffer')
            except NoSuchRowError:
                print('Chunk data not in write buffer for chunk index', chunk_index)
                (status, chunk_data) = self._download_chunk(inode, chunk_index)
                if status != 'success':
                    print('Download error:', status, chunk_data)
                    raise(FUSEError(errno.EIO))

            chunks_data += chunk_data

        # print(chunks_data)
        data_offset = offset % config.CHUNKSIZE
        # print(chunks_data[data_offset:data_offset+length])
        # final = chunks_data[data_offset:data_offset+length]
        # print('a', final, len(final), length)
        # return final
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
            try:
                chunk_data = self._get_row('SELECT data FROM write_buffer WHERE inode=? AND chunk_index=?', (fh, chunk_index))['data']
            except NoSuchRowError:
                # data not written before, try to download an existing chunk
                (status, response) = self._download_chunk(fh, chunk_index)
                if status == 'success':
                    chunk_data = response
                elif status == 'apierror' and response == 15:
                    # chunk does not exist
                    print('Chunk does not exist, using empty byte array')
                    chunk_data = b''
                else:
                    print('Unknown download error', status, response)
                    raise(FUSEError(errno.EREMOTEIO))

            # pad chunk with zero bytes if it's not the last chunk, so chunks align properly
            if chunk_index != end_chunk:
                print('padding chunk_index', chunk_index, 'end_chunk', end_chunk, 'with', config.CHUNKSIZE - len(chunk_data), 'bytes - before length', len(chunk_data))
                chunk_data = chunk_data + bytearray(config.CHUNKSIZE - len(chunk_data))
                print('length after padding', len(chunk_data))

            chunks_data += chunk_data

        data_offset = offset % config.CHUNKSIZE
        chunks_data = chunks_data[:data_offset] + buf + chunks_data[data_offset+len(buf):]

        for chunk_index in range(start_chunk, end_chunk + 1):
            data = chunks_data[chunk_index*config.CHUNKSIZE:(chunk_index+1)*config.CHUNKSIZE]
            timestamp = int(time.time())
            values = (fh, chunk_index, data, timestamp, data, timestamp)
            self.cursor.execute('INSERT INTO write_buffer (inode, chunk_index, data, last_update) VALUES (?,?,?,?) ON CONFLICT(inode, chunk_index) DO UPDATE SET data=?, last_update=?', values)

        self.write_lock.release()

        self._clear_write_buffer()

        return len(buf)


    async def release(self, fh):
        print('release', fh)
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

def check_connection():
    # Auth / connectivity check
    response = api.get('directoryListRoot')
    if response is None or not response[0]:
        print('Connection error, exiting')
        exit(1)
    else:
        print('Connection check successful')

if __name__ == '__main__':
    check_connection()

    options = parse_args()
    init_logging(options.debug)
    operations = Operations()

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=dsnfs')
    fuse_options.discard('default_permissions')
    if options.debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(operations, options.mountpoint, fuse_options)

    try:
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close(unmount=False)
        raise

    pyfuse3.close()
