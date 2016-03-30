# Copyright 2016 Randall Nortman
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import asyncio
from collections import deque
from concurrent.futures import FIRST_COMPLETED
import functools
import itertools
import struct
import io
import threading
import os
import socket
import sys

import logging

LOG = logging.getLogger(__name__)

from master import MsgHeader
from master import MSG_FLAGS_COMMITTED

class Follower:
    def __init__(self, reader, writer, loop):
        self.read_task = asyncio.async(self.reader(reader, writer, loop))
        return

    @asyncio.coroutine
    def reader(self, reader, writer, loop):
        #print('reader')
        try:
            for i in itertools.count():
                #print('read header', i, MsgHeader.size)
                header = yield from reader.readexactly(MsgHeader.size)
                msg_len, flags, seqno = MsgHeader.unpack(header)
                if flags & MSG_FLAGS_COMMITTED:
                    assert msg_len == MsgHeader.size
                else:
                    payload = yield from reader.readexactly(msg_len - MsgHeader.size)
                    #loop.call_later(0.5, self.send_ack, writer, seqno)
                    self.send_ack(writer, seqno)
        except:
            LOG.exception('Exception occured in Follower task')
            writer.close()
            raise

    def send_ack(self, writer, seqno):
        #print('follower ack', seqno)
        writer.write(MsgHeader.pack(MsgHeader.size, 0, seqno))
        return


    @classmethod
    @asyncio.coroutine
    def connect_and_run(cls, loop, host='127.0.0.1', port=8889):
        reader, writer = yield from asyncio.open_connection(host, port,
                                                            loop=loop)
        follower = cls(reader, writer, loop)
        return (yield from follower.read_task)

def main():
    #logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    #loop.set_debug(True)
    task = asyncio.async(Follower.connect_and_run(loop))
    print('main task', task)
    loop.run_until_complete(task)
    print('All done!', task.result())
    loop.close()
    return

if __name__ == '__main__':
    main()

