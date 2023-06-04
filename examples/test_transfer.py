#!/usr/bin/env python3

import sys

import radical.utils as ru

client = ru.zmq.Client(url=sys.argv[1])
res = client.request('stage', {'source': '/tmp/in',
                               'target': '/tmp/out'})
print(res)

