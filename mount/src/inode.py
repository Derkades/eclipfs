import errno

from pyfuse3 import FUSEError # pylint: disable=no-name-in-module

import api
import config

class Inode:

    @staticmethod
    def by_inode(inode):
        return Inode(api.get('inodeInfo', {'inode': inode}))

    @staticmethod
    def by_name(inode_p, name):
        return Inode(api.get('inodeInfo', {'inode_p': inode_p, 'name': name}))

    @staticmethod
    def by_mkdir(inode_p, name):
        return Inode(api.post('directoryCreate', {'directory': inode_p, 'name': name}))

    @staticmethod
    def by_mkfile(inode_p, name):
        return Inode(api.post('fileCreate', {'directory': inode_p, 'name': name}))

    def __init__(self, response):
        (success, response) = response

        if not success:
            if response == 2:
                raise(FUSEError(errno.ENOENT))
            elif response == 6:
                raise(FUSEError(errno.ENOENT))
            elif response == 9:
                raise(FUSEError(errno.EACCES))
            elif response == 22:
                raise(FUSEError(errno.ENOENT))
            elif response == 25:
                raise(FUSEError(errno.ENOENT))
            else:
                raise(Exception(response))

        if 'directory' in response:
            response = response['directory']
        if 'file' in response:
            response = response['file']
        self.response = response

    def inode(self):
        return self.response['inode']

    def name(self):
        return self.response['name']

    def path(self):
        return self.response['path']

    def inode_type(self):
        return self.response['type']

    def is_file(self):
        return self.inode_type() == 'f'

    def is_dir(self):
        return self.inode_type() == 'd'

    def size(self):
        return self.response['size']

    def crtime(self):
        return self.response['crtime']

    def ctime(self):
        return self.mtime()

    def mtime(self):
        return self.response['mtime']

    def parent_inode(self):
        return self.response['parent']

    def parent_obj(self):
        return Inode(self.parent_inode())

    @staticmethod
    def ceildiv(a, b):
        return -(-a // b)

    def chunks_count(self):
        return self.ceildiv(self.size(), config.CHUNKSIZE)

    def list_as_tuple(self):
        entries = []
        for d in self.response['directories']:
            entries.append((d['inode'], d['name']))
        for f in self.response['files']:
            entries.append((f['inode'], f['name']))
        return entries
