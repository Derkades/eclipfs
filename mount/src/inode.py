from typing import Literal

import errno
from pyfuse3 import FUSEError  # pylint: disable=no-name-in-module

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
        return Inode(api.post('directoryCreate',
                     {'directory': inode_p, 'name': name}))

    @staticmethod
    def by_mkfile(inode_p, name):
        return Inode(api.post('fileCreate',
                     {'directory': inode_p, 'name': name}))

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

    def inode(self) -> int:
        return self.response['inode']

    def name(self) -> str:
        return self.response['name']

    def path(self) -> str:
        return self.response['path']

    def inode_type(self) -> Literal['f', 'd']:
        return self.response['type']

    def is_file(self) -> bool:
        return self.inode_type() == 'f'

    def is_dir(self) -> bool:
        return self.inode_type() == 'd'

    def size(self) -> int:
        return self.response['size']

    def crtime(self) -> int:
        return self.response['crtime']

    def ctime(self) -> int:
        return self.mtime()

    def mtime(self) -> int:
        return self.response['mtime']

    def chunk_size(self) -> int:
        return self.response['chunk_size']

    def parent_inode(self) -> int:
        return self.response['parent']

    def parent_obj(self):
        return Inode(self.parent_inode())

    @staticmethod
    def ceildiv(a, b):
        return -(-a // b)

    def chunks_count(self) -> int:
        return self.ceildiv(self.size(), self.chunk_size())

    def children(self):
        return self.response['children'].items()

    def update_mtime(self, mtime: int):
        data = {
            'inode': self.inode(),
            'mtime': mtime
        }
        (success, response) = api.post('inodeUpdate', data=data)
        if not success:
            if response == 25:
                raise FUSEError(errno.ENOENT)
            else:
                raise FUSEError(errno.EREMOTEIO)
