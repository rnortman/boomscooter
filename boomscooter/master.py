# Copyright 2015 Randall Nortman
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
import functools
import itertools
import struct
import io
import threading
import os
import socket
import sys

import logging

MsgHeader = struct.Struct('!IIQ') # (size, flags, seqno)
AckMsg = struct.Struct('!IIQQ')   # (size, flags, refno, seqno)
IdxEntry = struct.Struct('!QQI')  # (seqno, offset, size)

class Log:
    def __init__(self, name, updated_cb=lambda: None):
        self.name = name
        self.head = (0,0)
        self.updated_cb = updated_cb
        #self.executor = ThreadPoolExecutor(1)
        self.logfile = None
        return

    @asyncio.coroutine
    def open(self, filename='log.boomscooter'):
        #self.logfile = yield from aiofiles.open(
        #    filename, 'w+b', buffering=-1, executor=self.executor)
        self.logfile = io.open(filename, 'w+b', buffering=0)
        self.idxfile = io.open(filename+'_idx', 'w+b', buffering=0)
        return

    @asyncio.coroutine
    def new_msg(self, msg):
        seqno, offset = self.head
        seqno += 1
        size = len(msg) + MsgHeader.size
        #yield from self.logfile.write(
        #    MsgHeader.pack(len(msg) + MsgHeader.size, 0, self.seqno) + msg)
        self.logfile.write(
            MsgHeader.pack(size, 0, seqno) + msg)
        self.idxfile.write(
            IdxEntry.pack(seqno, offset, size))
        self.head = (seqno, offset+size) # Atomic replacement
        #self.updated_cb()
        return seqno

MONITOR_PERIOD = 2

def start_monitor(loop, log):
    now = loop.time()
    loop.call_at(now+MONITOR_PERIOD, monitor, loop, log, now, log.head[0])
    return

def monitor(loop, log, prev_tick, prev_seqno):
    now = loop.time()
    seqno = log.head[0]
    print('monitor', (seqno - prev_seqno) / (now-prev_tick) , 'msgs/s')
    loop.call_at(now+MONITOR_PERIOD, monitor,
                 loop, log, now, seqno)

class Master:
    """Accepts messages from a producer"""


    def __init__(self, log, reader, writer):
        self.log = log
        self.read_task = asyncio.async(self.reader(reader, writer))
        #self.write_task = asyncio.async(self.writer(writer))
        return

    @asyncio.coroutine
    def reader(self, reader, writer):
        #print('reader')
        for i in itertools.count():
            #print('read header', i, MsgHeader.size)
            header = yield from reader.readexactly(MsgHeader.size)
            msg_len, msg_type, refno = MsgHeader.unpack(header)
            payload = yield from reader.readexactly(msg_len - MsgHeader.size)
            seqno = yield from self.log.new_msg(payload)
            writer.write(AckMsg.pack(AckMsg.size, 0, refno, seqno))
            #print('ack', i, refno, seqno)

    @asyncio.coroutine
    def writer(self, writer):
        print('writer')
        return

    @classmethod
    def connection_handler(cls, log, reader, writer):
        cls(log, reader, writer)
        return

class FeedServer:
    """Accepts connections from consumers and launches Feeders for them"""

    def __init__(self, log, bind_addr=('127.0.01', 8889), family=socket.AF_INET):
        self.log = log
        self.socket = socket.socket(family=family)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(bind_addr)
        self.socket.listen(10)
        self.loop = asyncio.new_event_loop()
        self.log_updated = asyncio.Event(loop=self.loop)
        self.thread = threading.Thread(target=self.loop_thread, daemon=True)
        self.thread.start()
        return

    def loop_thread(self):
        asyncio.set_event_loop(self.loop)
        self.loop.create_task(self.listen())
        self.loop.run_forever()

    def log_updated_cb(self):
        self.loop.call_soon_threadsafe(self._log_just_updated)
        return

    def _log_just_updated(self):
        self.log_updated.set()
        self.log_updated.clear()
        return

    @asyncio.coroutine
    def listen(self):
        while True:
            client, addr = yield from self.loop.sock_accept(self.socket)
            feeder = Feeder(self.log, self.log_updated, client)
            yield from feeder.start(self.loop)

    pass



ConsumerHandshakeMsg = struct.Struct('!Q')


class Feeder:
    """Feeds messages to a consumer"""

    def __init__(self, log, log_updated, socket):
        self.log = log
        self.log_updated = log_updated
        self.socket = socket
        self.cur_offset = None
        self.writeable = asyncio.Event()
        return

    @asyncio.coroutine
    def start(self, loop):
        reader, writer = yield from asyncio.open_connection(
            sock=self.socket, loop=loop)
        yield from self.reader(reader, loop)
        #loop.add_writer(self.write_cb)
        return

    @asyncio.coroutine
    def reader(self, stream, loop):
        handshake = yield from stream.readexactly(ConsumerHandshakeMsg.size)
        after_seqno = ConsumerHandshakeMsg.unpack(handshake)[0]
        yield from self.writer(loop, 0)
        return

    def install_write_watcher(self, loop):
        loop.add_writer(self.socket.fileno(), self.writeable.set)
        return

    @asyncio.coroutine
    def writer(self, loop, after_seqno):
        last_offset = 0
        log_seqno, log_offset = self.log.head
        while log_seqno <= after_seqno:
            last_offset = log_offset
            yield from self.log_updated.wait()
            log_seqno, log_offset = self.log.head
        # TODO FIXME: Handle starting from an offset, and then do that
        # here as well in case we somehow missed the correct offset
        # while waiting to start.  This all requires, presumably, an
        # offset index to be implemented.

        self.install_write_watcher(loop)

        while True:
            yield from self.writeable.wait()
            # FIXME: What happens when the client disconnects?  Will I
            # sleep forever, or will the socket become writable and
            # then raise an exception?

            log_seqno, log_offset = self.log.head
            to_send = log_offset - last_offset
            if to_send > 0:
                try:
                    last_offset += os.sendfile(
                        self.socket.fileno(), self.log.logfile.fileno(),
                        last_offset, to_send)
                except BlockingIOError:
                    pass
            else:
                loop.remove_writer(self.socket.fileno())
                yield from self.log_updated.wait()
                self.install_write_watcher(loop)

def main():
    #logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    log = Log('scootr')
    #feed_server = FeedServer(log)
    #log.updated_cb = feed_server.log_updated_cb
    #loop.run_until_complete(log.open('/dev/null'))
    loop.run_until_complete(log.open())
    coro = asyncio.start_server(
        functools.partial(Master.connection_handler, log),
        '127.0.0.1', 8888, loop=loop)
    start_monitor(loop, log)
    loop.run_until_complete(coro)
    loop.run_forever()
    loop.close()
    return

if __name__ == '__main__':
    main()
