Implement the most minimal server possible
- No topics/partitions
- No replicas
- No compression

Then add replicas, but without automatic failover.  Then add automatic
failover in pre-determined order.  Then add dynamic leader election.

-  32: msg len
-  16: msgtype (or have 8-bit flags, one of which determines log message vs protocol?)
-   8: Version num
-   8: 
-  64: Seqno


Server structure:

asyncio starts server listener that accepts producer connections.
We're going to separate producer streams from consumer streams, so if
you want to be both, you need to have two connections.  (Revisit this
later).  For now, let's also put the consumer listener in the same
thread.  If we find that sendfile() blocks on I/O, we can move some of
the consumer stuff into a separate thread later.

Producer handler:

No need for a handshake.  It reads bytes until it gets a full message,
at which point it:

- Assigns a seqno from a server-global Log instance
- Appends message to log file (does not flush)
- Updates Log instance max seqno and file offset
- Appends to offset index (does not flush)
- Sends a response back to the producer
- Flags a Condition with notify_all(), which consumers are waiting on.

Consumer handler:

On connect the consumer sends a handshake requesting messages starting
at a particular seqno.

- The consumer handler checks the current max seqno of the Log; if
  it's not greater than the requested offset, set connection offset to
  Log size and skip to main loop.

- Handler mmaps() the offset index and looks up the byte offset of the
  given seqno.  (Might do this via the Log so that we can maybe have a
  shared cache of some sort later.)

- Main loop:

  - While (consumer offset is less than size of Log), submit a
    sendfile() from current offset of either the remainder of the file
    or a configurable maximum sendfile block size.  Update consumer
    offset with number of bytes sent.

  - Wait on Log's Condition object.
