
import os

import radical.utils as ru


from .base import HandlerBase


# ------------------------------------------------------------------------------
#
class StagingHandler(HandlerBase):

    #
    def __init__(self):

        self._boto3 = ru.import_module('boto3')
        self._s3    = self._boto3.client('s3')


    # --------------------------------------------------------------------------
    #
    def transfer(src, tgt, flags=0):
        """
        :param src: str - path to local file
        :param tgt: str - bucket name
        :param flags:   - not supported
        """

        if flags != 0:
            raise ValueError('flags not supported')

        response = s3_client.upload_file(src, 'staging_area', tgt)


# ------------------------------------------------------------------------------

