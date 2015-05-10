======================
 Boomscooter Protocol
======================

Protocol Roles
==============

- Server: One of Leader, Replica:

  - Leader: Accepts entries from producer(s); assigns seqnos (if
	necessary); publishes to cluster replicas; publishes to
	subscribers.

  - Replica: Follows leader; publishes to subscribers; forwards
	publish requests to leader?

- Subscriber: Receives committed messages starting at a particular
  seqno (optionally ending at a particular seqno or after a certain
  number of messages, or a certain size?)

- Producer: Creates new msgs and submits to leader; assigns seqnos if
  solitary producer for channel.


Subscriber-Server Protocol (full)
=================================

Subscriber connects to server.  Do I want to enable multiplexing
different logs/channels/partitions over a single TCP connection?  This
adds some overhead, because you have to prefix each batch of messages
with identifying information, and also you (probably) need to know how
many messages will be in each batch.  It would be beneficial in the
case of a large number of logs, each of which has relatively low
volume.  This would otherwise require a large number of TCP
connections.  But in the case of a small number of high-volume logs,
you would be better off dedicating a connection to each.

We could always support both types and let the client decide which to
use.  One approach would be to avoid having to pre-determine the
number of messages in each batch and instead send "log switch"
messages whenever a switch is required.  This requires a magic byte
that allows us to differentiate a channel switch message from a log
message.  This suggests we should separate the magic byte from the
version num, so we have an API ID or msgtype field and a separate
version byte.


OK, non-multiplexed version first.  Only one message is required,
specifying the start offset and optionally the end offset or number of
messages.  Might consider having a "refill" message that adds to the
number of remaining messages, as a kind of credit-based flow control.


Subscriber-Server Protocol (notifications only)
===============================================

This mode is optimized for situations where the client has access to
the underlying files, so the server does not send the content of
messages; instead it just notifies the subscriber when new data is
available or when new data files are created.  These notifications
should include the new file size and highest seqno.  Notifications can
be sent immediately after each message or "batched" and sent (at most)
once per configurable time interval.  This may end up being a subset
of the leader-replica protocol.


Same as above, except no message content will be in the replies.


Handshake
---------

- Version num / magic byte
- Seqno
- Optionally a "channel" string, which can be used for prefix-matched
  subscriptions and possibly for partitioning
- Optionally a "key" for indexing?
- Checksum?
- Opaque ref num?  Or just use seqno for this?
- Payload, which is just a binary blob as far as the protocol is concerned


Producer-Leader Protocol
========================

