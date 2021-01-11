---
layout: post
title: "Stuff your logs!"
author: Paul Khuong
date: 2021-01-11 15:11:23 -0500
comments: true
categories:
---

Nine months ago, we embarked on a format migration for the persistent
(on-disk) representation of variable-length strings like symbolicated
call stacks in the [Backtrace](https://www.backtrace.io) server.  We
chose a variant of
[consistent overhead byte stuffing (COBS)](http://www.stuartcheshire.org/papers/COBSforToN.pdf),
a [self-synchronising code](https://en.wikipedia.org/wiki/Self-synchronizing_code),
for the metadata (variable-length as well).
This choice let us improve our software's resilience to data
corruption in local files, and then parallelise data hydration, which
improved startup times by a factor of 10... without any hard
migration from the old to the current on-disk data format.

In this post, I will explain why I believe that the representation of
first resort for binary logs (write-ahead, recovery, replay, or
anything else that may be consumed by a program) should be
self-synchronising, backed by this migration and by prior experience
with COBS-style encoding.  I will also describe the
[specific algorithm (available under the MIT license)](https://github.com/backtrace-labs/stuffed-record-stream)
we implemented for our server software.

This encoding offers low space overhead for framing, fast encoding and
faster decoding, resilience to data corruption, and a restricted form
of random access.  Maybe it makes sense to use it for your own data!

What is self-synchronisation, and why is it important?
------------------------------------------------------

A [code is self-synchronising](https://en.wikipedia.org/wiki/Self-synchronizing_code)
when it's always possible to unambiguously detect where a valid code
word (record) starts in a stream of symbols (bytes).  That's a
stronger property than prefix codes like Huffman codes, which only
detect when valid code words end.  For example, the UTF-8
encoding is self-synchronising, because initial bytes and continuation
bytes differ in their high bits.  That's why it's possible to decode
multi-byte code points when tailing a UTF-8 stream.

The UTF-8 code was designed for small integers (Unicode code points),
and can double the size of binary data.  Other encodings are more
appropriate for arbitrary bytes; for example,
[consistent overhead byte stuffing (COBS)](http://www.stuartcheshire.org/papers/COBSforToN.pdf),
a self-synchronising code for byte streams, offers a worst-case
space overhead of one byte plus a 0.4% space blow-up.

Self-synchronisation is important for binary logs because it lets us
efficiently (with respect to both run time and space overhead) frame
records in a simple and robust manner... and we want simplicity and
robustness because logs are most useful when something has gone wrong.

Of course, the storage layer should detect and correct errors, but
things will sometimes fall through, especially for on-premises
software, where no one fully controls deployments.  When that happens,
graceful partial failure is preferable to, e.g., losing all the
information in a file because one of its pages went to the great bit
bucket in the sky.

One easy solution is to spread the data out over multiple files or
blobs.  However, there's a trade-off between keeping data
fragmentation and file metadata overhead in check, and minimising the
blast radius of minor corruption.  Our server must be able to run
on isolated nodes, so we can't rely on design options available to
replicated systems... plus bugs tend to be correlated across replicas,
so there is something to be said for defense in depth, even with
distributed storage.

When each record is converted with a
[self-synchronising code](https://en.wikipedia.org/wiki/Self-synchronizing_code)
like
[COBS](https://en.wikipedia.org/wiki/Consistent_Overhead_Byte_Stuffing)
before persisting to disk, we can decode all records that weren't
directly impacted by corruption, exactly like decoding a stream of
mostly valid UTF-8 bytes.  Any form of corruption will only make
us lose the records whose bytes were corrupted, and, at most, the two records
that immediately precede or follow the corrupt byte range.  This
guarantee covers overwritten data (e.g., when a network switch flips a
bit, or a read syscall silently errors out with a zero-filled page),
as well as bytes removed or garbage inserted in the middle of log
files.

The coding doesn't store redundant information: replication or erasure
coding is the storage layer's responsibility.  It instead guarantees
to *always* minimise the impact of corruption, and only lose records
that were adjacent to or directly hit by corruption.

A COBS encoding for log records achieves that by unambiguously
separating records with a reserved byte (e.g., 0), and re-encoding
each record to avoid that separator byte.  A reader can thus assume
that potential records start and end at a log file's first and last
bytes, and otherwise look for separator bytes to determine where to
cut all potential records.  These records may be invalid: a
separator byte could be introduced or removed by corruption, and the
contents of a correctly framed record may be corrupt.  When
that happens, readers can simply scan for the next separator byte and
try to validate that new potential record.  The decoder's state resets
after each separator byte, so any corruption is "forgotten" as soon as
the decoder finds valid a separator byte.

On the write side, the encoding logic is simple (a couple dozen lines
of C code), and uses a predictable amount of space, as expected from
an algorithm suitable for microcontrollers.

Actually writing encoded data is also easy:
on POSIX filesystems, we can make sure each record is delimited (e.g.,
prefixed with the delimiter byte), and issue a
[regular `O_APPEND` write(2)](https://pubs.opengroup.org/onlinepubs/007904875/functions/write.html).
Vectored writes can even insert delimiters without copying in
userspace.  Realistically, our code is probably less stable than
operating system and the hardware it runs on, so we make sure our
writes make it to the kernel as soon as possible, and let `fsync`s
happen on a timer.

When a write errors out, we can blindly (maybe once or twice) try
again: the encoding is independent of the output file's state.  When a
write is cut short, we can still issue the same[^suffix] write call,
without trying to "fix" the short write: the encoding and the
read-side logic already protect against that kind of corruption.

[^suffix]: If you append with the delimiter, it probably makes sense to special-case short writes and also prepend with the delimiter after failures, in order to make sure readers will observe a delimiter before the new record.

What if multiple threads or processes write to the same log file?
When we [open with `O_APPEND`](https://pubs.opengroup.org/onlinepubs/007904875/functions/open.html),
the operating system can handle the rest.  This doesn't make
contention disappear, but at least we're not adding a bottleneck in
userspace on top of what is necessary to append to the same file.
Buffering is also trivial: the encoding is independent of the state of
the destination file, so we can always concatenate buffered records
and write the result with a single syscall.

This simplicity also plays well  with
[high-throughput I/O primitives like `io_uring`](https://kernel.dk/io_uring.pdf), and with
[blob stores that support appends](https://docs.microsoft.com/en-us/rest/api/storageservices/append-block):
independent workers can concurrently queue up blind append requests and
retry on failure.
There's no need for application-level mutual exclusion or rollback.

Fun tricks with robust readers
------------------------------

Our log encoding will recover from bad bytes, as long as readers can
detect and reject invalid records as a whole; the processing logic
should also handle duplicated valid records.  These are table stakes
for a reliable log consumer.

In our variable-length metadata use case, each record describes a
symbolicated call stack, and we recreate in-memory data structures by
replaying an append-only log of metadata records, one for each
unique call stack.  The hydration phase handles invalid records by
ignoring (not recreating) any call stack with corrupt metadata,
but only those call stacks.  That's definitely an improvement over the
previous situation, where corruption in a size header would prevent us
from decoding the remainder of the file, and thus make us forget about
*all* call stacks stored at file offsets after the corruption.

Of course, losing data should be avoided, so we are careful to
`fsync` regularly and recommend reasonable storage configurations.
However, one can only make data loss unlikely, not impossible (if only
due to fat fingering), especially when cost is a factor. With the COBS
encoding, we can recover gracefully and automatically from any
unfortunate data corruption event.

We can also turn this robustness into new capabilities.

It's often useful to process the tail of a log at a regular cadence.
For example, I once maintained a system that regularly tailed hourly
logs to update approximate views. One could support that use case with
length footers. COBS framing lets us instead scan
for a valid record from an arbitrary byte location, and read the rest
of the data normally.

When logs grow large enough, we want to process them in parallel.  The
standard solution is to shard log streams, which unfortunately couples
the parallelisation and storage strategies, and adds complexity to the
write side.

COBS framing lets us parallelise readers independently of the writer.
The downside is that the read-side code and I/O patterns are now more
complex, but, all other things being equal, that's a trade-off I'll
gladly accept, especially given that our servers run on independent
machines and store their data in files, where reads are fine-grained
and latency relatively low.

A parallel COBS reader partitions a data file arbitrarily (e.g., in
fixed size chunks) for independent workers.  A worker will scan for
the first valid record starting inside its assigned chunk, and handle
every record that *starts* in its chunk. Filtering on the start byte
means that a worker may read past the logical end of its chunk, when
it fully decodes the last record that starts in the chunk: that's how
we unambiguously assign a worker to every record, including records
that straddle chunk boundaries.

Random access even lets us implement a form of binary or interpolation
search on raw unindexed logs, when we know the records are (k-)sorted
on the search key!  This lets us, e.g., access the metadata for a few
call stacks without parsing the whole log.

Eventually, we might also want to truncate our logs.

Contemporary filesystems like XFS (and even Ext4) support large sparse
files.  For example, sparse files can reach $2^{63} - 1$ bytes on
XFS with a minimal metadata-only footprint: the on-disk data for such
sparse files is only allocated when we issue actual writes.  Nowadays,
we can [sparsify files after the fact](https://lwn.net/Articles/415889/),
and convert ranges of non-zero data into zero-filled "holes" in order
to release storage without messing with file offsets
(or even atomically
[collapse old data away](https://lwn.net/Articles/589260/)).

Filesystems can only execute these operations at coarse granularity,
but that's not an issue for our readers: they must merely remember to
skip sparse holes, and the decoding loop will naturally handle any
garbage partial record left behind.

The original consistent overhead byte stuffing scheme
-----------------------------------------------------

[Cheshire and Baker's original byte stuffing scheme](http://www.stuartcheshire.org/papers/COBSforToN.pdf)
targets small machines and slow transports (amateur radio and
phone lines).  That's why it bounds the amount of buffering needed to
254 bytes for writers and 9 bits of state for readers, and attempts to
minimise space overhead, beyond its worst-case bound of 0.4%.

The algorithm is also reasonable. The encoder buffers data until it
encounters a reserved 0 byte (a delimiter byte), or there are 254
bytes of buffered data.  Whenever the encoder stops buffering, it
outputs a block whose contents are described by its first byte.  If
the writer stopped buffering because it found a reserved byte, it
emits one byte with `buffer_size + 1` before writing and clearing the
buffer.  Otherwise, it outputs 255 (one more than the buffer size),
followed by the buffer's contents.

On the decoder side, we know that the first byte of each block
describes its size and decoded value (255 means 254 bytes of literal
data, any other value is one more than the number of literal bytes to
copy, followed by a reserved 0 byte).  We denote the end of a record
with an implicit delimiter: when we run out of data to decode, we
should have just decoded an extra delimiter byte that's not really
part of the data.

With framing, an encoded record surrounded by delimiters thus looks
like the following

```
|0   |blen|(blen - 1) literal data bytes....|blen|literal data bytes ...|0    |
```

The delimiting "0" bytes are optional at the beginning and end of a
file, and each `blen` size prefix is one byte with value in
$[1, 255]$.  A value $\mathtt{blen} \in [1, 254]$ represents
a block $\mathtt{blen} - 1$ literal bytes, followed by an implicit
 0 byte.  If we instead have $\mathtt{blen} = 255$, we
have a block of $254$ bytes, without any implicit byte.  Readers
only need to remember how many bytes remain until the end of the
current block (eight bits for a counter), and whether they should insert
an implicit 0 byte before decoding the next block (one binary flag).

[Backtrace](https://www.backtrace.io)'s word stuffing variant
-------------------------------------------------------------

We have different goals for the software we write at
[Backtrace](https://www.backtrace.io).  For our logging use case, we
pass around fully constructed records, and we want to issue a single
write syscall per record, with periodic `fsync`.[^why-not-fewer]
Buffering is baked in, so there's no point in making sure we can work
with a small write buffer.  We also don't care as much about the space
overhead (the worst-case bound is already pretty good) as much as we
do about encoding and decoding speed.

[^why-not-fewer]: High-throughput writers should batch records.  We do syscall-per-record because the write load for the current use case is so sporadic that any batching logic would usually end up writing individual records.  For now, batching would introduce complexity and bug potential for a minimal impact on write throughput.

These different design goals lead us to an updated hybrid word/byte
stuffing scheme:

1. it uses a two-byte "reserved sequence," carefully chosen to appear
   infrequently in our data
2. the size limit for the first block is slightly smaller (252 bytes instead
   of 254)
3. ... but the limit for every subsequent block is much larger,
   65008 bytes, for an asymptotic space overhead of 0.0031%.

This hybrid scheme improves encoding and decoding speed compared to
COBS, and even marginally improves the asymptotic space overhead.  At
the low end, the worst-case overhead is only slightly worse than that
of traditional COBS: we need three additional bytes, including the
framing separator, for records of 252 bytes or fewer, and five bytes
for records of 253-64260 bytes.

In the past, I've seen
["word" stuffing schemes](https://issues.apache.org/jira/browse/AVRO-27)
aim to reduce the run-time overhead of COBS codecs by scaling up
the COBS loops to work on two or four bytes at a time.  However,
a byte search is trivial to vectorise, and there is no guarantee that
frameshift corruption will be aligned to word boundaries (for example,
POSIX allows short writes of an arbitrary number of bytes).

### Much ado about two bytes

Our hybrid word-stuffing looks for a reserved two-byte delimiter
sequence at arbitrary byte offsets.  We must still conceptually
process bytes one at a time, but delimiting with a pair of bytes
instead of with a single byte makes it easier to craft a delimiter
that's unlikely to appear in our data.

Cheshire and Baker do the opposite, and use a frequent byte (0) to
eliminate the space overhead in the common case.  We care a lot more
about encoding and decoding speed, so an unlikely delimiter makes more
sense for us.  We picked `0xfe 0xfd` because that sequence doesn't
appear in small integers (unsigned, two's complement, varint, single
or double float) regardless of endianness, nor in valid UTF-8 strings.

Any positive integer with `0xfe 0xfd` (`254 253`) in its byte must be
around $2^{16}$ or more.  If the integer is instead negative in
little-endian two's complement, `0xfe 0xfd` equals -514 as a
little-endian `int16_t`, and -259 in big endian (not as great, but not
nothing).  Of course, the sequence could appear in two adjacent
`uint8_t`s, but otherwise, for `0xfe` or `0xfd` can only appear in
most significant byte of large 32- or 64-bit integers (unlike `0xff`,
which could be sign extension for, e.g., -1).

Any [(U)LEB varint](https://en.wikipedia.org/wiki/LEB128) that
includes `0xfe 0xfd` must span at least 3 bytes (i.e., 15 bits),
since both these bytes have the most significant bit set to 1.
Even a negative SLEB has to be at least as negative as
$- 2^{14} = -16384$.

For floating point types, we can observe that `0xfe 0xfd` in the
significand would represent an awful fraction in little or big
endian, so can only happen for the IEEE-754 representation of large
integers (approximately $\pm 2^{15}$).  If we instead assume
that `0xfd` or `0xfe` appear in the sign and exponent fields, we find
either very positive or very negative exponents (the exponent is
biased, instead of complemented).  A semi-exhaustive search confirms
that the smallest integer-valued single float that includes the
sequence is 32511.0 in little endian and 130554.0 in big endian;
among integer-valued double floats, we find 122852.0 and 126928.0
respectively.

Finally, the sequence isn't valid UTF-8 because both `0xfe` and `0xfd`
have their top bit set (indicating a multi-byte code point), but neither
looks like a continuation byte: the two most significant bits are
`0b11` in both cases, while UTF-8 continuations must have `0b10`.

### Encoding data to avoid the reserved sequence

Consistent overhead byte stuffing rewrites reserved 0 bytes away by
counting the number of bytes from the beginning of a record until the
next 0, and storing that count in a block size header followed by the
non-reserved bytes, then resetting the counter, and doing the same
thing for the remaining of the record. A complete record is stored as
a sequence of encoded blocks, none of which include the reserved
byte 0.  Each block header spans exactly one byte, and must never
itself be 0, so the byte count is capped at 254, and incremented by
one (e.g., a header value of 1 represents a count of 0); when the
count in the header is equal to the maximum, the decoder knows that
the encoder stopped short without finding a 0.

With our two-byte reserved sequence, we can encode the size of each
block in radix 253 (`0xfd`); given a two-byte header for each block, sizes
can go up to $253^2 - 1 = 64008$.  That's a reasonable granularity
for `memcpy`.  This radix conversion replaces the off-by-one weirdness
in COBS: that part of the original algorithm merely encodes values
from $[0, 254]$ into one byte while avoiding the reserved byte 0.

A two-byte size prefix is a bit ridiculous for small records (ours
tend to be on the order of 30-50 bytes). We thus encode the first
block specially, with a single byte in $[0, 252]$ for the size
prefix.  Since the reserved sequence `0xfe 0xfd` is unlikely to appear in
our data, the encoding for short record often boils down to adding a
`uint8_t` length prefix.

A framed encoded record now looks like
```
|0xfe|0xfd|blen|blen literal bytes...|blen_1|blen_2|literal bytes...|0xfe|0xfd|
```

The first `blen` is in $[0, 252]$ and tells us how many literal
bytes follow in the initial block.  If the initial $\mathtt{blen} =
252$, the literal bytes are immediately followed by the next block's
decoded contents.  Otherwise, we must first append an implicit `0xfe
0xfd` sequence... which may be the artificial reserved sequence that
mark the end of every record.

Every subsequent block comes with a two-byte size prefix, in little-endian
radix-253.  In other words, `|blen_1|blen_2|` represents the
block size $\mathtt{blen}_{ 1 } + 253 \cdot \mathtt{blen}_{ 2 }$, where
$\mathtt{blen}_{\{1, 2\}} \in [0, 252]$.  Again, if the block
size is the maximum encodable size, $253^2 - 1 = 64008$, we
have literal data followed by the next block; otherwise, we must
append a `0xfe 0xfd` sequence to the output before
moving on to the next block.

The encoding algorithm is only a bit more complex than for the
original COBS scheme.

Assume the data to encode is suffixed with an artificial two-byte
reserved sequence `0xfe 0xfd`.

For the first block, look for the reserved sequence in the first 252
bytes.  If we find it, emit its position (must be less than 251) in
one byte, then all the data bytes up to but not including the reserved
sequence, and enter regular encoding after the reserved sequence.  If
the sequence isn't in the first block, emit `252`, followed
by 252 bytes of data, and enter regular encoding after those bytes.

For regular (all but the first) blocks, look for the reserved sequence in
the next 64008 bytes.  If we find it, emit the sequence's byte offset
(must be less than 64008) in little-endian radix 253, followed by the
data up to but not including the reserved sequence, and skip that sequence
before encoding the rest of the data.  If we don't find the reserved
sequence, emit 64008 in radix 253 (`0xfc 0xfc`), copy the next 64008
bytes of data, and encode the rest of the data without skipping anything.

Remember that we conceptually padded the data with a reserved sequence at
the end.  This means we'll always observe that we fully consumed the
input data at a block boundary.  When we encode the block that stops
at the artificial reserved sequence, we stop (and frame with a reserved
sequence to delimit a record boundary).

You can find our [implementation in the stuffed-record-stream repository](https://github.com/backtrace-labs/stuffed-record-stream/blob/main/src/word_stuff.c).

When writing short records, we already noted that the encoding step is
often equivalent to adding a one-byte size prefix.  In fact, we can
encode and decode all records of size up to $252 + 64008 = 64260$
bytes in place, and only ever have to slide the initial 252-byte
block: whenever a block is shorter than the maximum length (252 bytes
for the first block, 64008 for subsequent ones), that's because we
found a reserved sequence in the decoded data.  When that happens, we
can replace the reserved sequence with a size header when encoding,
and undo the substitution when decoding.

Our code does not implement these optimisations because encoding and
decoding stuffed bytes aren't bottlenecks for our use case, but it's
good to know that we're nowhere near the performance ceiling.

A resilient record stream on top of word stuffing
-------------------------------------------------

The stuffing scheme only provides resilient framing.  That's
essential, but not enough for an abstract stream or sequence of
records.  At the very least, we need checksums in order to detect
invalid records that happen to be correctly encoded (e.g., when a
block's literal data is overwritten).

Our pre-stuffed records start with the little-endian header

```
struct record_header {
        uint32_t crc;
        uint32_t generation;
};
```

where `crc` is the `crc32c` of whole record, including the
header,[^initialize-your-crc] and `generation` is a yet-unused
arbitrary 32-bit payload that we added for forward compatibility.
There is no size field: the framing already handles that.

[^initialize-your-crc]: We overwrite the `crc` field with `UINT32_MAX` before computing a checksum for the header and its trailing data.  It's important to avoid zero prefixes because the result of crc-ing a 0 byte into a 0 state is... 0.

The remaining bytes in a record are an arbitrary payload.  We use
[protobuf messages](https://developers.google.com/protocol-buffers/docs/reference/proto2-spec)
to help with schema evolution (and keep messages small and flat for
decoding performance), but there's no special relationship between the
stream of word-stuffed records and the payload's format.

[Our implementation](https://github.com/backtrace-labs/stuffed-record-stream/blob/main/include/record_stream.h)
let writers output to buffered `FILE` streams, or directly to file descriptors.

Buffered streams offer higher write throughput, but are only safe
when the caller handles synchronisation and flushing; we use them
as part of a commit protocol that
[fsync](https://pubs.opengroup.org/onlinepubs/009695399/functions/fsync.html)s
and publishes files with
[atomic `rename` syscalls](https://pubs.opengroup.org/onlinepubs/009695399/functions/rename.html).

During normal operations, we instead write to file descriptors opened
with `O_APPEND` and a background fsync worker: in practice, the
hardware and operating system are more stable than our software, so
it's more important that encoded records immediately make it to the
kernel than all the way to persistent storage.  We also avoid batching
write syscalls because we would often have to wait several minutes if
not hours to buffer more than two or three records.

For readers, we can either read from a buffer, or `mmap` in a file,
and read from the resulting buffer.  While we expose a linear iterator
interface, we can also override the start and stop byte offset of an
iterator; we use that capability to replay logs in parallel.  Finally,
when readers advance an iterator, they can choose to receive a raw data
buffer, or have it decoded with a protobuf message descriptor.

What's next?
------------

We have happily been using this log format for more than nine months
to store a log of metadata records that we replay every time the
[Backtrace](https://www.backtrace.io) server restarts.

Decoupling writes from the parallel read strategy let us improve our
startup time incrementally, without any hard migration.  Serialising
with flexible schemas
([protocol buffers](https://developers.google.com/protocol-buffers/docs/reference/proto2-spec))
also made it easier to start small and slowly add optional metadata,
and only enforce a hard switch-over when we chose to delete backward
compatibility code.

This piecemeal approach let us transition from a length-prefixed data
format to one where all important metadata lives in a resilient record
stream, without any breaking change.  We slowly added more metadata to
records and eventually parallelised loading from the metadata record
stream, all while preserving backward and forward compatibility.  Six
months after the initial roll out, we flipped the switch and made the
new, more robust, format mandatory; the old length-prefixed files
still exist, but are now bags of arbitrary checksummed data bytes,
with metadata in record streams.

In the past nine months, we've gained a respectable amount of pleasant
operational experience with the format. Moreover, while performance is
good enough for us (the parallel loading phase is currently
dominated by disk I/O and parsing in `protobuf-c`), we also know
there's plenty of headroom: our records are short enough that they can
usually be decoded without any write, and always in place.

We're now starting laying the groundwork to distribute our single-node
embedded database and making it interact more fluently with other data
stores.  The first step will be generating a
[change data capture stream](https://materialize.com/change-data-capture-part-1/),
and re-using the word-stuffed record format was an obvious choice.

Word stuffing is simple, efficient, and robust.  If you can't just
defer to a real database (maybe you're trying to write one yourself)
for your log records, give it a shot!  Feel free to
[play with our code](https://github.com/backtrace-labs/stuffed-record-stream)
if you don't want to roll your own.
