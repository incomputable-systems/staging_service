# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import glob
import os
import shutil

from unittest import TestCase

import incomputable.staging_service as inc_ss


# ------------------------------------------------------------------------------
#
class TestStagingService(TestCase):

    _cleanup_files = []

    #
    @classmethod
    def tearDownClass(cls) -> None:

        for p in cls._cleanup_files:
            for f in glob.glob(p):
                if os.path.isdir(f):
                    try:
                        shutil.rmtree(f)
                    except OSError as e:
                        print('[ERROR] %s - %s' % (e.filename, e.strerror))
                else:
                    os.unlink(f)

    # --------------------------------------------------------------------------
    #
    def test_basics(self):

        self.assertEqual(True, True)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestStagingService()
    tc.test_basics()


# ------------------------------------------------------------------------------

