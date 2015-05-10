import asyncio
import itertools
import time
import struct

import logging

MsgHeader = struct.Struct('!IIQ')
AckMsg = struct.Struct('!IIQQ')

TEST_PAYLOAD = b'hello world!' * 1
TEST_PAYLOAD = b'!' * 16384
#TEST_PAYLOAD = b''

TEST_MSG = MsgHeader.pack(MsgHeader.size + len(TEST_PAYLOAD), 0, 0) + TEST_PAYLOAD

MAX_IN_FLIGHT = 1000

NUM_MSGS = int(1E5)

class Producer:
    @asyncio.coroutine
    def connect(self, loop, host='127.0.0.1', port=8888):
        reader, writer = yield from asyncio.open_connection('127.0.0.1', 8888,
                                                            loop=loop)
        self.in_flight = asyncio.BoundedSemaphore(MAX_IN_FLIGHT)
        return (asyncio.async(self.writer(writer)),
                asyncio.async(self.reader(reader)))

    @asyncio.coroutine
    def writer(self, writer):
        self.start_time = time.time()
        for i in itertools.count():
            #print('waiting in flight')
            yield from self.in_flight.acquire()
            #print('fill', i, self.in_flight.locked(), writer.transport.get_write_buffer_size())
            writer.write(TEST_MSG)
            yield from writer.drain()

    @asyncio.coroutine
    def reader(self, reader):
        for i in itertools.count():
            ack = yield from reader.readexactly(AckMsg.size)
            msg_len, msg_type, refno, seqno = AckMsg.unpack(ack)
            #print('ack', i, msg_len, msg_type, refno, seqno)
            self.in_flight.release()
        duration = self.end_time - self.start_time
        rate = NUM_MSGS / duration
        print('Done!', NUM_MSGS, duration, rate)

def main():
    #logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    #loop.set_debug(True)
    for i in range(1): asyncio.async(Producer().connect(loop))
    loop.run_forever()
    loop.close()
    return

if __name__ == '__main__':
    main()
