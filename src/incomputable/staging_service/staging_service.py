
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.utils as ru

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

        ru.zmq.Server.__init__(self)

        self.register_request('stage', self._request_stage)

        self._handlers = list()

        for name in ['saga']:

          # try:
            if True:
                modname = 'incomputable.staging_service.handlers.%s' % name
                module  = ru.import_module(modname)
                handler = module.StagingHandler()
                self._handlers.append(handler)

          # except Exception as e:
          #     print('skip handler %s: %s' % (name, repr(e)))


    # --------------------------------------------------------------------------
    #
    def _request_stage(self, request):
        '''
        receive staging requests, execute them, and reply with completion
        messages on the control channel
        '''

        uid     = request.get('uid')    or ru.generate_id('staging')
        action  = request.get('action') or TRANSFER
        flags   = request.get('flags')  or 0
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

        for handler in self._handlers:

            try:
                if action == TRANSFER:
                    handler.transfer(src, tgt, flags)

                request['state'] = DONE

            except Exception as e:
                print('skip handler %s for request %s' % (handler.name, uid))

        if request['state'] != DONE:
            self._prof.prof('staging_fail', uid=prof_id, msg=uid)
            raise RuntimeError('no handler found for request %s' % uid)

        self._prof.prof('staging_stop', uid=prof_id, msg=uid)


# ------------------------------------------------------------------------------

