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
from collections import deque
from concurrent.futures import ThreadPoolExecutor
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

USE_EXECUTOR = False
DO_WRITES = True

MsgHeader = struct.Struct('!IIQ') # (size, flags, seqno)
AckMsg = struct.Struct('!IIQQ')   # (size, flags, refno, seqno)
IdxEntry = struct.Struct('!QQI')  # (seqno, offset, size)

MSG_FLAGS_COMMITTED = 1

class LogFile:
    def __init__(self, name, updated_cb=lambda: None):
        self.name = name
        self.head = (0,0)
        self.updated_cb = updated_cb
        self._executor = ThreadPoolExecutor(1)
        self.logfile = None
        self._txns_in_flight = deque()
        self._txn_committed = asyncio.Condition()
        return

    @asyncio.coroutine
    def open(self, filename='log.boomscooter'):
        #self.logfile = yield from aiofiles.open(
        #    filename, 'w+b', buffering=-1, executor=self.executor)
        self.logfile = io.open(filename, 'w+b', buffering=0)
        self.idxfile = io.open(filename+'_idx', 'w+b', buffering=0)
        return

    @asyncio.coroutine
    def new_msg(self, payload):
        seqno, offset = self.head
        seqno += 1
        size = len(payload) + MsgHeader.size
        # Atomic replacement of self.head allows concurrent read,
        # though the changes must happen in this event loop.
        self.head = (seqno, offset+size)
        msg = MsgHeader.pack(size, 0, seqno) + payload
        if USE_EXECUTOR:
            write_future = asyncio.get_event_loop().run_in_executor(
                self._executor,
                self._write_msg, msg)
        else:
            self._write_msg(msg)
            write_future = None
        txn = (seqno, write_future, offset, size)
        self._txns_in_flight.append(txn)
        return seqno, msg

    def _write_msg(self, msg):
        """May block on disk I/O; to be called in executor"""
        if DO_WRITES:
            self.logfile.write(msg)
        return

    @asyncio.coroutine
    def commit_msg(self, seqno):
        # This will raise IndexError if we attempt to commit when
        # nothing is in flight
        txn_seqno, write_future, offset, size \
            = self._txns_in_flight.popleft()
        while txn_seqno != seqno:
            # Can't commit out of order; wait our turn
            with (yield from self._txn_committed):
                yield from self._txn_committed.wait()
            txn_seqno, write_future, offset, size \
                = self._txns_in_flight.popleft()
        assert txn_seqno == seqno
        # Wait for our local write to be done; raise exception if
        # there was one
        #yield from write_future
        # Write to the index file; this marks it as committed locally.
        if USE_EXECUTOR:
            asyncio.get_event_loop().run_in_executor(
                self._executor,
                self.idxfile.write,
                IdxEntry.pack(seqno, offset, size))
        elif DO_WRITES:
            self.idxfile.write(
                IdxEntry.pack(seqno, offset, size))
        with (yield from self._txn_committed):
            self._txn_committed.notify_all()
        #self.updated_cb()
        return


class LogManager:
    def __init__(self, name,
                 min_synced_followers,
                 updated_cb=lambda: None,
    ):
        self.name = name
        self.logfile = LogFile(name, updated_cb)
        self.followers = []
        self.min_synced_followers = min_synced_followers
        return

    @asyncio.coroutine
    def open(self, filename='log.boomscooter'):
        return (yield from self.logfile.open(filename))

    @asyncio.coroutine
    def new_msg(self, payload):
        #print('LogManager.new_msg head')
        seqno, msg = yield from self.logfile.new_msg(payload)
        #print('LogManager.new_msg seqno', seqno)
        if True:
            follower_futures = [
                f.notify_new_msg(seqno, msg)
                for f in self.followers
                ]
            num_synced = 0
            while num_synced < self.min_synced_followers:
                assert len(follower_futures) > 0
                done, follower_futures \
                    = yield from asyncio.wait(
                        follower_futures,
                        return_when=FIRST_COMPLETED)
                num_synced += sum(
                    1 if x.result() else 0
                    for x in done)
        yield from self.logfile.commit_msg(seqno)
        if True:
            for f in self.followers:
                asyncio.async(f.notify_commit_msg(seqno))
        return seqno, msg

    def add_follower(self, follower):
        self.followers.append(follower)

class FollowerHandler:
    def __init__(self, log_manager, reader, writer):
        print('follower init', log_manager)
        self.log_manager = log_manager
        self._txns_in_flight = deque()
        self.read_task = asyncio.async(self.reader(reader, writer))
        self.writer = writer
        self.log_manager.add_follower(self)
        return

    @asyncio.coroutine
    def notify_new_msg(self, seqno, msg):
        try:
            future = asyncio.Future()
            txn = (seqno, future)
            self._txns_in_flight.append(txn)
            self.writer.write(msg)
            return (yield from future)
        except:
            LOG.exception('Exception writing to follower')
            
    @asyncio.coroutine
    def notify_commit_msg(self, seqno):
        try:
            #print('commit msg', seqno)
            self.writer.write(
                MsgHeader.pack(MsgHeader.size, MSG_FLAGS_COMMITTED, seqno))
            return
        except:
            LOG.exception('Exception writing to follower')

    @asyncio.coroutine
    def reader(self, reader, writer):
        #print('reader')
        try:
            for i in itertools.count():
                #print('Follower read header', i, MsgHeader.size)
                header = yield from reader.readexactly(MsgHeader.size)
                msg_len, flags, seqno = MsgHeader.unpack(header)
                if msg_len != MsgHeader.size:
                    return (yield from self.peer_is_crazy(
                        'Sent message of wrong size'))
                try: 
                    head_seqno, future = self._txns_in_flight.popleft()
                except IndexError:
                    return (yield from self.peer_is_crazy(
                        'Tried to ACK but no ACK expected'))
                if seqno != head_seqno:
                    future.set_result(False)
                    return (yield from self.peer_is_crazy(
                        'Tried to ACK out of order '
                        'or otherwise unexpected seqno'))
                future.set_result(True)
        except:
            LOG.exception('Exception occured in Follower task')
            writer.close()
            raise

    @asyncio.coroutine
    def peer_is_crazy(self, reason):
        LOG.warn('Follower peer is crazy: ' + reason)
        while self._txns_in_flight:
            seqno, future = self._txns_in_flight.popleft()
            future.set_result(False)
        self.writer.close()

    @classmethod
    def connection_handler(cls, log, reader, writer):
        print('Follower connection_handler')
        cls(log, reader, writer)
        return


MONITOR_PERIOD = 2

def start_monitor(loop, log):
    now = loop.time()
    loop.call_at(now+MONITOR_PERIOD, monitor, loop, log, now, log.logfile.head[0])
    return

def monitor(loop, log, prev_tick, prev_seqno):
    now = loop.time()
    seqno = log.logfile.head[0]
    print('monitor', (seqno - prev_seqno) / (now-prev_tick) , 'msgs/s')
    loop.call_at(now+MONITOR_PERIOD, monitor,
                 loop, log, now, seqno)

class Master:
    """Accepts messages from a producer"""


    def __init__(self, log, reader, writer):
        print('master init', log)
        self.log = log
        self.read_task = asyncio.async(self.reader(reader, writer))
        #self.write_task = asyncio.async(self.writer(writer))
        return

    @asyncio.coroutine
    def reader(self, reader, writer):
        #print('reader')
        try:
            for i in itertools.count():
                #print('read header', i, MsgHeader.size)
                header = yield from reader.readexactly(MsgHeader.size)
                msg_len, msg_type, refno = MsgHeader.unpack(header)
                payload = yield from reader.readexactly(msg_len - MsgHeader.size)
                seqno, msg = yield from self.log.new_msg(payload)
                writer.write(AckMsg.pack(AckMsg.size, 0, refno, seqno))
                #print('ack', i, refno, seqno)
        except:
            LOG.exception('Exception occured in Master task')
            writer.close()
            raise

    @asyncio.coroutine
    def writer(self, writer):
        print('writer')
        return

    @classmethod
    def connection_handler(cls, log, reader, writer):
        print('connection_handler')
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
        log_seqno, log_offset = self.log.logfile.head
        while log_seqno <= after_seqno:
            last_offset = log_offset
            yield from self.log_updated.wait()
            log_seqno, log_offset = self.log.logfile.head
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

            log_seqno, log_offset = self.log.logfile.head
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
    #loop.set_debug(True)
    log = LogManager(
        name='scootr',
        min_synced_followers=1,
    )
    #feed_server = FeedServer(log)
    #log.updated_cb = feed_server.log_updated_cb
    #loop.run_until_complete(log.open('/dev/null'))
    loop.run_until_complete(log.open())
    master_coro = asyncio.start_server(
        functools.partial(Master.connection_handler, log),
        '127.0.0.1', 8888, loop=loop)
    follower_coro = asyncio.start_server(
        functools.partial(FollowerHandler.connection_handler, log),
        '127.0.0.1', 8889, loop=loop)
    start_monitor(loop, log)
    asyncio.async(master_coro)
    asyncio.async(follower_coro)
    #loop.run_until_complete(coro)
    loop.run_forever()
    loop.close()
    return

if __name__ == '__main__':
    main()
