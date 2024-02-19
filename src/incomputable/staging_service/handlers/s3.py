
import os

import radical.utils as ru

from .base import HandlerBase


# ------------------------------------------------------------------------------
#
class StagingHandler(HandlerBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._rs   = ru.import_module('radical.saga')
        self._rsfs = self._rs.filesystem

        self._session       = self._rs.Session()
        self._cache_lock    = ru.Lock()
        self._saga_fs_cache = dict()


    # --------------------------------------------------------------------------
    #
    def transfer(self, src, tgt, flags):

        # open the staging directory for the target, and cache it
        # url used for cache: tgt url w/o path
        tmp      = self._rs.Url(tgt)
        tmp.path = '/'
        key      = str(tmp)

        with self._cache_lock:
            if key in self._saga_fs_cache:
                fs = self._saga_fs_cache[key]

            else:
                fs = self._rsfs.Directory(key, session=self._session)
                self._saga_fs_cache[key] = fs

        flags |= self._rsfs.CREATE_PARENTS
        if os.path.isdir(src) or src.endswith('/'):
            flags |= self._rsfs.RECURSIVE

        fs.copy(src, tgt, flags=flags)


# ------------------------------------------------------------------------------

