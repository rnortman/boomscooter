=============
 Boomscooter
=============

The log server/protocol should have very limited scope.  Definitely no
key/value semantics, and for now no garbage collection or archiving.
This is an insert-only paradigm; no updates or deletes.  Every message
has a sequence number, which can be either an integer or a 64-bit
timestamp of some sort.  It must be strictly ordered and monotonically
increasing (which is potentially problematic with timestamps!).
Seqnos can be assigned by the server in the case of multiple
submitting clients (otherwise strict ordering cannot be maintained) or
assigned by the client in the case of a single submitter.

A message has:

-  16: msgtype (or have 8-bit flags, one of which determines log message vs protocol?)
-   8: Version num
-   8: pad?
-  32: crc32 of header?
-  64: Seqno
- Optionally a "channel" string, which can be used for prefix-matched
  subscriptions and possibly for partitioning
- Optionally a "key" for indexing?
- Checksum?
- Opaque ref num?  Or just use seqno for this?
- Payload, which is just a binary blob as far as the protocol is concerned

That's all well and good, but how to handle batching and compressing
messages?  When the batch is created on the producer, each message in
it has an "offset" starting from 0.  When the batch is sent to the
server, the producer says how many messages are in it.  The server
then assigns the base seqno, and increments the seqno by
num_messages+1.  The seqno of each individual msg is then base_seqno +
msg_offset.  The producer does not check to be sure the num_msgs is
correct; each consumer should validate this during unpacking, and
should discard extra msgs.

Inside the batch, the "seqno" field could be used to hold the batch
offset, or else seqno could be set to 0 and the offset just increases
by 1 for each message in the batch.  I like the latter, because
otherwise we would need logic to check that indeed the offset in the
seqno field is correct.  Better to use message order as canonical and
just ignore seqno inside the batch.  (The client API should fill in
seqno as the batch is unpacked.)

Message format / serialization
==============================

We could use a serialization format like Cap'n Proto for the message
header, but I don't like that.  I think we should just specify the
structure manually and use custom validation, because I don't want to
create a required dependancy on a serialization protocol.

On the other hand, we will probably need to serialize configuration
and control commands in some way anyway, for which a standard
serialization format makes some sense.

Yes.  Yes, let's use capnp.  But then the question is whether to use
this for only protocol and config, or also for the messages
themselves, or perhaps only for the message headers.  Blob size limits
may be quite relevant here -- capnp limits blobs to 512MiB.  That is
probably OK for most use cases of boomscooter.  While it would be nice
to be able to say that msg size is unlimited or has a crazy limit like
2^64 bytes, in practice I think 512MiB is OK.  Any applications which
need something bigger can workaround this by splitting large objects
across several messages.

Uh oh, the big problem with capnp is that its message framing is not
going to interact well with things like sendfile, and also that its
I/O does not (yet) integrate with asyncio.  The read/write methods do
not return number of bytes written, and it's hard (expensive) to
pre-calculate the message size, so this makes it extra work to figure
out file offsets to store in external indexes.


Right, I think that for the messages themselves, both on-disk and
on-wire, we need a custom binary framing/header format.  It should be
optimized for mmap()-style operations, meaning we need to pay
attention to alignment.  From python, we will just use the struct
module for dealing with these, unless this becomes a performance
bottleneck, in which case Cython will come to the rescue.  The on-wire
format must be the same so that we can use sendfile() to send large
chunks over the network.

The question, then, is what to use for the rest of the wire protocol
-- the stuff that isn't messages.  One option is to have messages sent
out-of-band, which means two different TCP connections for each
client.  That is ugly.


Modes of access
===============

Don't underestimate the value of direct file access by multiple
readers for performance.  Should be even better than sendfile().
Should probably test file access via nfs/sshfs/cifs vs. using the
client/server protocol.


Indexes
=======

It's going to be very important to be able to find the file and offset
of messages, so that readers know where to start reading, with as few
disk accesses as possible.  For this, it may make sense to
(optionally) store an index alongside each data file.  For building
this index, it may make sense for each message to have a "key" field
(e.g., a timestamp) to index.  The big question is the index
structure.  The simplest would be an ordered list of (seqno,key)
pairs.  If the keys are fixed length, this basically maps directly to
a C array and can be very efficiently searched if the keys are
monotonically increasing (timestamps).  Of course, if you can
guarantee that, then the key should really be used as the seqno.
Otherwise a sequential scan would be necessary.  Still, if messages
are large, then a seq scan of the index will be quicker than the data
file.

In a lot of applications, though, you are going to need a btree or
hash index on the key, not on seqno.  We might make this external to
boomscooter, or an optional add-on, to keep things simple.

Another option is to simply record the min and max key values in each
data file.  If keys are almost but not quite monotonic (timestamps),
this allows you to at least quickly find the right file to start your
scan in.  If data files are kept smallish, this might be plenty good
enough for most apps.


Batched messages will need to be unpacked in order to make any kind of
indexing work (incl. the simple min/max).


Compression
===========

Instead of compressing the file in "batch" with some kind of archival
process that operates only on closed/old files, it may turn out to be
more efficient to compress the files at the same time that the
uncompressed files are being written, and then when the file is "old"
enough that we want to get rid of the uncompressed version, we simply
delete the uncompressed file, leaving the compressed one.  The
reasoning is that if we compress in batch, then we're going to have to
read the entire uncompressed file in order to compress it, whereas if
we compress as we go, it will already be in page cache (or the
compression process can get it via the client/server protocol or
directly through some sort of shared memory with the server process).

Partitioning
============

The Kafka notion of topic/partition is a little odd.  Really, from the
protocol/server perspective, each log is a partition, and the
different partitions of a topic have nothing to do with one another.
The only feature that the server provides which really depends on the
difference is automatic assignment of partitions to particular nodes
for load balancing, though I'm not sure Kafka even implements that.
All of this could be done by splitting the log name into two pieces;
the topic name and the partition.

The ability to combine multiple partitions into single protocol
messages is of questionable value, because of the fact that only the
leader of a partition can accept a producer request, and so the
producer is going to be having to have connections to lots of
different servers and will need to batch its requests to different
servers in an arbitrary(ish) fashion.

How much extra work really is involved in being the leader for a
partition vs a replica?  It seems that the answer is probably not
much.  It may make sense for one node of the cluster to be the leader
for *all* partitions in the cluster, and to acheive load balancing by
requiring or encouraging consumers to connect only to replicas.

Some benchmarking is in order.  I think we should start with a very
minimal protocol that does have any notion of topic or partition --
just a single log per cluster -- and do some benchmarks to see what
type of load balancing makes sense.  Also to benchmark file sharing
vs. network.  (I think file sharing is going to win at least in any
case where each system hosts more than one consumer (because then they
share page cache), and certainly whenever the server and consumers are
on the same host.


Possible names:
===============

from http://pacificforestfoundation.org/glossary.html

boom boat
highballer
silviculture, or some variation on "silvan"?
timber beast
timber cruiser (for a log analyzer component)
wanigan
yarder (excellent, but used by a ruby gem already)
ponderosa?

from http://www.history.com/shows/ax-men/articles/logging-lingo

boomscooter

http://www.soperwheeler.com/about-us/education/logging-terminology/
http://www.puresimplicity.net/~heviarti/Logging_Terms.html
http://en.wiktionary.org/wiki/Appendix:Glossary_of_lumberjack_jargon


batteau
