
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.saga            as rs
import radical.saga.filesystem as rsfs
import radical.utils           as ru

from .constants import NEW, ACTIVE, DONE, FAILED, CANCELED
from .constants import TRANSFER, COPY, LINK, MOVE


# ------------------------------------------------------------------------------
#
class StagingService(ru.zmq.Server):
    '''
    FIXME
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._session       = rs.Session()
        self._cache_lock    = ru.Lock()
        self._saga_fs_cache = dict()

        ru.zmq.Server.__init__(self)

        self.register_request('stage', self._request_stage)


    # --------------------------------------------------------------------------
    #
    def _request_stage(self, request):
        '''
        receive staging requests, execute them, and reply with completion
        messages on the control channel
        '''

        # TODO: respect flags in directive

        action  = request['action']
        flags   = request['flags']
        uid     = request['uid']
        src     = request['source']
        tgt     = request['target']
        prof_id = request.get('prof_id')   # staging on behalf of this entity

        assert action in [COPY, LINK, MOVE, TRANSFER]

        self._prof.prof('staging_start', uid=prof_id, msg=uid)

        if action in [COPY, LINK, MOVE]:
            self._prof.prof('staging_fail', uid=prof_id, msg=uid)
            raise ValueError("invalid action '%s' on stager" % action)

        self._log.info('transfer %s', src)
        self._log.info('      to %s', tgt)

        # open the staging directory for the target, and cache it
        # url used for cache: tgt url w/o path
        tmp      = rs.Url(tgt)
        tmp.path = '/'
        key      = str(tmp)

        with self._cache_lock:
            if key in self._saga_fs_cache:
                fs = self._saga_fs_cache[key]

            else:
                fs = rsfs.Directory(key, session=self._session)
                self._saga_fs_cache[key] = fs

        flags |= rsfs.CREATE_PARENTS
        if os.path.isdir(src) or src.endswith('/'):
            flags |= rsfs.RECURSIVE

        fs.copy(src, tgt, flags=flags)

        request['state'] = DONE

        self._prof.prof('staging_stop', uid=prof_id, msg=uid)


# ------------------------------------------------------------------------------

