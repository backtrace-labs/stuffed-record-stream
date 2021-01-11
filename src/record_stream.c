/*
 * Copyright 2021 Backtrace I/O, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#define _GNU_SOURCE /* For SEEK_DATA */
#include "record_stream.h"

#include <assert.h>
#include <errno.h>
#include <smmintrin.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "word_stuff.h"

#define CRDB_ARRAY_SIZE(X) (sizeof(X) / sizeof(*(X)))

#define CRDB_ERROR_SET_(E, M, N, ...) ({ _crdb_error_set(E, M, (N)); false; })

#define crdb_error_set(E, M, ...) CRDB_ERROR_SET_((E), (M), ##__VA_ARGS__, 0)

/*
 * Fill the record_header.crc field with CRC_INITIAL_VALUE when
 * computing the checksum: crc32c is vulnerable to 0-prefixing,
 * so we make sure the initial bytes are non-zero.
 */
#define CRC_INITIAL_VALUE ((uint32_t)-1)

struct record_header {
	uint32_t crc;
	uint32_t generation;
};

struct write_record {
	struct record_header header;
	uint8_t data[CRDB_RECORD_STREAM_MAX_LEN];
};

struct read_record {
	struct record_header header;
	uint8_t data[CRDB_RECORD_STREAM_BUF_LEN];
};

static inline void
_crdb_error_set(struct crdb_error *error, const char *message,
    unsigned long long n)
{

	if (error == NULL)
		return;

	error->message = message;
	error->error = n;
	return;
}

/**
 * This is our internal reference implementation.  You probably want
 * something else that has higher performance.
 */
static uint32_t
crdb_crc32c(const void *buf, size_t len)
{
        uint32_t acc = 0;
        size_t i;

        for (i = 0; i + sizeof(uint32_t) <= len; i += sizeof(uint32_t)) {
                uint32_t bytes;

                memcpy(&bytes, (const uint8_t *)buf + i, sizeof(bytes));
                acc = _mm_crc32_u32(acc, bytes);
        }

        for (; i < len; i++)
                acc = _mm_crc32_u8(acc, ((const uint8_t *)buf)[i]);

        return acc;
}

/**
 * Encodes the write record to `encoded[0 ... *encoded_size - 1]`.
 */
static bool
encode_record(
    uint8_t encoded[static CRDB_WORD_STUFFED_BOUND(sizeof(struct write_record))],
    size_t *encoded_size, struct write_record *record, size_t data_len,
    crdb_error_t *ce)
{
	enum { MAX_ENCODED = CRDB_WORD_STUFFED_BOUND(sizeof(struct write_record)) };
	size_t record_size;
	uint8_t *write_ptr;

	static_assert((size_t)MAX_ENCODED <= CRDB_RECORD_STREAM_BUF_LEN,
	    "The maximum encoded size must fit in the read record size limit.");

	if (data_len > CRDB_RECORD_STREAM_MAX_LEN)
		return crdb_error_set(ce, "crdb_record_stream data too long");

	record->header.crc = CRC_INITIAL_VALUE;
	record_size = sizeof(struct record_header) + data_len;
	record->header.crc = crdb_crc32c(record, record_size);

	assert(crdb_word_stuffed_size(record_size, true) <= MAX_ENCODED);

	write_ptr = crdb_word_stuff_encode(encoded, record, record_size);
	/*
	 * The beginning and end of file act as implicit headers, and
	 * we simply have to separate records with the 2-byte header;
	 * we are free to write a header before the current record, or
	 * after, in preparation for the next write.
	 *
	 * We prefer to write the header preemptively because we
	 * observe that corruption mostly happens at the tail of the
	 * file: having the header in place early makes it more likely
	 * that it will make it to persistent storage before a crash.
	 */
	write_ptr = crdb_word_stuff_header(write_ptr);
	*encoded_size = write_ptr - encoded;
	return true;
}

/**
 * Repeatedly attempts to write `buf` to `fd`, which is expected to be
 * in O_APPEND mode.
 *
 * The buffer is word-stuffed and ends with a header for the next record.
 */
static bool
append_to_fd(int fd, const void *buf, size_t count, crdb_error_t *ce)
{
	static const size_t num_tries = 3;
	uint8_t header[CRDB_WORD_STUFF_HEADER_SIZE];
	struct iovec iov[] = {
		{
			/*
			 * The first write does not include a header:
			 * we assume the previous write inserted one
			 * for us.
			 */
			.iov_base = header,
			.iov_len = 0
		},
		{
			.iov_base = (void *)buf,
			.iov_len = count,
		}
	};
	size_t expected = count;
	ssize_t written;
	int err;
	/* Flip to true when at least one write was short. */
	bool partial_write = false;

	for (size_t i = 0; i < num_tries; i++) {
		const uint8_t *end;

		written = writev(fd, iov, CRDB_ARRAY_SIZE(iov));
		if ((size_t)written == expected)
			break;

		/* We failed without writing anything; just retry. */
		if (written <= 0)
			continue;

		/*
		 * If the first write was short, we should definitely
		 * make sure there's a header before the new record:
		 * we can't assume the previous write left one for us.
		 *
		 * It is however safe to leave a partially written
		 * record behind: the read-side will correctly detect
		 * it as corrupt.  If another writer succeeded just
		 * after the short write, we may also lose that
		 * record... but we expect write failures to be sticky
		 * (media failure or exhausted storage quotas).
		 */
		partial_write = true;
		end = crdb_word_stuff_header(header);
		assert(end == header + sizeof(header));
		iov[0].iov_len = sizeof(header);
		expected = count + sizeof(header);
	}

	/*
	 * If we failed and left some partial records behind, try to
	 * at least leave a header for the next writer.
	 */
	err = errno;
	if (partial_write == true && (size_t)written != expected) {
		ssize_t r;

		/*
		 * This write is best-effort: if it fails, that's
		 * life, and there's not much we can do against what
		 * is probably a storage media or quota problem.
		 */
		r = write(fd, iov[0].iov_base, iov[0].iov_len);
		(void)r;
	}

	if (written < 0)
		return crdb_error_set(ce, "record_stream write(2) failed.", err);

	if ((size_t)written != expected)
		return crdb_error_set(ce, "Short write in record_stream.");

	return true;
}

/**
 * Dumps an encoded version of `record` to `fd`. The record's header
 * will be updated in-place to contain the correct crc.
 *
 * We guarantee that the encoded record (header + data) fits in
 * CRDB_RECORD_STREAM_BUF_LEN bytes.
 */
static bool
record_stream_append_record(int fd, struct write_record *record,
    size_t data_len, crdb_error_t *ce)
{
	uint8_t encoded[CRDB_WORD_STUFFED_BOUND(sizeof(*record))];
	size_t encoded_size;

	if (encode_record(encoded, &encoded_size, record, data_len, ce) == false)
		return false;

	return append_to_fd(fd, encoded, encoded_size, ce);
}

static bool
record_stream_write_record(FILE *stream, struct write_record *record,
    size_t data_len, crdb_error_t *ce)
{
	uint8_t encoded[CRDB_WORD_STUFFED_BOUND(sizeof(*record))];
	size_t encoded_size;
	size_t written;

	if (encode_record(encoded, &encoded_size, record, data_len, ce) == false)
		return false;

	written = fwrite(encoded, encoded_size, 1, stream);
	if (written != 1)
		return crdb_error_set(ce, "crdb_record_stream fwrite(3) failed.",
		    errno);

	return true;
}

static bool
fd_ends_with_header(int fd,
    const uint8_t header[static CRDB_WORD_STUFF_HEADER_SIZE])
{
	uint8_t buf[CRDB_WORD_STUFF_HEADER_SIZE];
	ssize_t ret;

	if (lseek(fd, -(off_t)sizeof(buf), SEEK_END) < 0)
		return false;

	do {
		ret = read(fd, buf, sizeof(buf));
	} while (ret == -1 && errno == EINTR);

	if ((size_t)ret != sizeof(buf))
		return false;

	return memcmp(buf, header, sizeof(buf)) == 0;
}

bool
crdb_record_stream_append_initial(int fd, crdb_error_t *ce)
{
	uint8_t header[CRDB_WORD_STUFF_HEADER_SIZE];
	uint8_t *end;

	end = crdb_word_stuff_header(header);
	assert(end == header + sizeof(header));
	/* Nothing to do if we definitely have a header at the end. */
	if (fd_ends_with_header(fd, header))
		return true;

	/* Otherwise, it's always safe to append a header. */
	return append_to_fd(fd, header, sizeof(header), ce);
}

bool
crdb_record_stream_write_initial(FILE *stream, crdb_error_t *ce)
{
	uint8_t header[CRDB_WORD_STUFF_HEADER_SIZE];
	uint8_t *end;
	size_t written;

	end = crdb_word_stuff_header(header);
	assert(end == header + sizeof(header));
	written = fwrite(header, sizeof(header), 1, stream);
	if (written != 1)
		return crdb_error_set(ce,
		    "crdb_record_stream initial fwrite(3) failed.",
		    errno);

	return true;
}

bool
crdb_record_stream_append_buf(int fd, uint32_t generation,
    const uint8_t *buf, size_t len, crdb_error_t *ce)
{
	struct write_record record = {
		.header.generation = generation,
	};

	if (len > CRDB_RECORD_STREAM_MAX_LEN)
		return crdb_error_set(ce, "crdb_record_stream data too long");

	memcpy(&record.data, buf, len);
	return record_stream_append_record(fd, &record, len, ce);
}

bool
crdb_record_stream_write_buf(FILE *stream, uint32_t generation,
    const uint8_t *buf, size_t len, crdb_error_t *ce)
{
	struct write_record record = {
		.header.generation = generation,
	};

	if (len > CRDB_RECORD_STREAM_MAX_LEN)
		return crdb_error_set(ce, "crdb_record_stream data too long");

	memcpy(&record.data, buf, len);
	return record_stream_write_record(stream, &record, len, ce);
}

#ifdef HAS_PROTOBUF_C
bool
crdb_record_stream_append_msg(int fd, uint32_t generation,
    const ProtobufCMessage *message, crdb_error_t *ce)
{
	struct write_record record = {
		.header.generation = generation,
	};
	size_t packed_size;
	size_t serialized_size;

	packed_size = protobuf_c_message_get_packed_size(message);
	if (packed_size > CRDB_RECORD_STREAM_MAX_LEN)
		return crdb_error_set(ce,
		    "crdb_record_stream message too large.");

	serialized_size = protobuf_c_message_pack(message, record.data);
	assert(serialized_size <= packed_size);
	return record_stream_append_record(fd, &record, serialized_size, ce);
}

bool
crdb_record_stream_write_msg(FILE *stream, uint32_t generation,
    const ProtobufCMessage *message, crdb_error_t *ce)
{
	struct write_record record = {
		.header.generation = generation,
	};
	size_t packed_size;
	size_t serialized_size;

	packed_size = protobuf_c_message_get_packed_size(message);
	if (packed_size > CRDB_RECORD_STREAM_MAX_LEN)
		return crdb_error_set(ce,
		    "crdb_record_stream message too large.");

	serialized_size = protobuf_c_message_pack(message, record.data);
	assert(serialized_size <= packed_size);
	return record_stream_write_record(stream, &record, serialized_size, ce);
}
#endif /* HAS_PROTOBUF_C */

void
crdb_record_stream_iterator_init_buf(struct crdb_record_stream_iterator *it,
    const uint8_t *buf, size_t size)
{

	*it = (struct crdb_record_stream_iterator) {
		.cursor = buf,
		.end = buf + size,
		.stop_at = buf + size,
		.begin = buf,
		.first_nonzero = buf,
		.first_record = true,
	};
	return;
}

static const uint8_t *
find_first_nonzero(const uint8_t *cursor, const uint8_t *end)
{

	while (cursor < end && cursor[0] == 0)
		cursor++;

	return cursor;
}

bool
crdb_record_stream_iterator_init_fd(struct crdb_record_stream_iterator *it,
    int fd, crdb_error_t *ce)
{
	struct stat st;
	void *mapped;
	off_t first_data = -1;

	if (fstat(fd, &st) == -1)
		return crdb_error_set(ce, "failed to fstat record stream",
		    errno);

	if (st.st_size <= 0) {
		crdb_record_stream_iterator_init_buf(it, NULL, 0);
		return true;
	}

	/* Skip any sparse hole at the head. */
	first_data = lseek(fd, 0, SEEK_DATA);
	mapped = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if (mapped == MAP_FAILED)
		return crdb_error_set(ce, "failed to mmap record stream",
		    errno);

	*it = (struct crdb_record_stream_iterator) {
		.cursor = mapped,
		.end = (const uint8_t *)mapped + st.st_size,
		.stop_at = (const uint8_t *)mapped + st.st_size,
		.begin = mapped,
		.mapped = mapped,
		.map_size = st.st_size,
		.first_record = true,
	};

	/* If we found a hole, advance the cursor. */
	if (first_data > 0) {
		if (first_data >= st.st_size)
			first_data = st.st_size;

		it->cursor += first_data;
	}

	/*
	 * And now, skip zeros: we know any valid record starts with a
	 * (non-zero) two-byte header.
	 */
	it->cursor = it->first_nonzero = find_first_nonzero(it->cursor, it->end);
	return true;
}

void
crdb_record_stream_iterator_deinit(struct crdb_record_stream_iterator *it)
{

	if (it->mapped != NULL)
		munmap(it->mapped, it->map_size);
	return;
}

size_t
crdb_record_stream_iterator_size(const struct crdb_record_stream_iterator *it)
{

	return it->end - it->begin;
}

bool
crdb_record_stream_iterator_locate_at(struct crdb_record_stream_iterator *it,
    size_t start_offset)
{

	/*
	 * We can't rewrite before the first byte that might have
	 * useful data, or after the byte at which we want to stop
	 * decoding.
	 */
        if (start_offset < (size_t)(it->first_nonzero - it->begin) ||
	    start_offset > (size_t)(it->stop_at - it->begin))
		return false;

	if (start_offset == (size_t)(it->first_nonzero - it->begin)) {
		it->first_record = true;
		it->cursor = it->first_nonzero;
		return true;
	}

	it->first_record = false;
	it->cursor = it->begin + start_offset;
	return true;
}

void
crdb_record_stream_iterator_stop_at(struct crdb_record_stream_iterator *it,
    size_t stop_offset)
{

	if (stop_offset > (size_t)(it->end - it->begin))
		return;

	it->stop_at = it->begin + stop_offset;
	return;
}

static bool
crc_matches(struct read_record *record, size_t total_len)
{
	uint32_t expected = record->header.crc;

	record->header.crc = CRC_INITIAL_VALUE;
	return expected == crdb_crc32c(record, total_len);
}

/**
 * Consumes and attempts to decode the next record.
 *
 * @param it a non-empty iterator.
 *
 * @return the size of the decoded record data on success, -1 on failure.
 */
static ssize_t
record_stream_iterator_next_record(struct crdb_record_stream_iterator *it,
    struct read_record *dst)
{
	const uint8_t *encoded_data;
	size_t encoded_len;
	size_t decoded_len;

	/*
	 * Skip to the next header, except for the initial record,
	 * which may not have any prefixing header: we actually write
	 * *trailers* to protect against the most common form of
	 * corruption we observe, with garbage appended to valid data.
	 */
	if (it->first_record == true) {
		it->first_record = false;

		it->header = it->cursor;
		encoded_data = it->cursor;
	} else {
		const uint8_t *first_header;

		first_header = crdb_word_stuff_header_find(it->cursor,
		    it->end - it->cursor);
		/* No header found -> consume everything and bail. */
		if (first_header >= it->stop_at)
			goto eof;

		it->header = first_header;
		encoded_data = first_header + CRDB_WORD_STUFF_HEADER_SIZE;
		assert(encoded_data <= it->end);
	}

	/*
	 * If we found data, but it starts too far in the directory
	 * file, return eof.
	 */
	if (it->header >= it->stop_at)
		goto eof;

	{
		const uint8_t *next_header;

		next_header = crdb_word_stuff_header_find(encoded_data,
		    it->end - encoded_data);
		/*
		 * We found where the next record starts; decode
		 * everything up to that byte.
		 */
		it->cursor = next_header;
		encoded_len = next_header - encoded_data;
	}

	/*
	 * We moved the cursor to the next encoded record.  We just
	 * have to decode and validate the data.
	 */

	/* This is clearly too much data. Reject early. */
	if (encoded_len > CRDB_RECORD_STREAM_BUF_LEN)
		return -1;

	/* Unstuff the bytes. */
	{
		uint8_t *decoded_begin = (uint8_t *)dst;
		uint8_t *decoded_end;

		/*
		 * Decoding never expands the number of bytes, so we
		 * know this won't overflow dst.
		 */
		decoded_end = crdb_word_stuff_decode(decoded_begin,
		    encoded_data, encoded_len);
		if (decoded_end == NULL)
			return -1;
		decoded_len = decoded_end - decoded_begin;
	}

	/*
	 * Make sure we decoded a full header, and that the header's
	 * checksum is correct.
	 */
	if (decoded_len < sizeof(dst->header) ||
	    crc_matches(dst, decoded_len) == false)
		return -1;

	return decoded_len - sizeof(dst->header);

eof:
	it->cursor = it->end;
	return -1;
}

/**
 * Writes the next valid record to `dst`.
 *
 * @return the size of the decoded record data on success, -1 on failure.
 */
static ssize_t
record_stream_iterator_next(struct crdb_record_stream_iterator *it,
    struct read_record *dst)
{

	while (it->cursor < it->stop_at) {
		ssize_t r;

		r = record_stream_iterator_next_record(it, dst);
		if (r >= 0)
			return r;
	}

	it->cursor = NULL;
	it->end = NULL;
	return -1;
}

bool
crdb_record_stream_iterator_next_buf(struct crdb_record_stream_iterator *it,
    uint32_t *generation, uint8_t dst[static CRDB_RECORD_STREAM_BUF_LEN],
    size_t *len)
{
	struct read_record buf;
	ssize_t payload_size;

	*generation = 0;
	*len = 0;
	payload_size = record_stream_iterator_next(it, &buf);
	if (payload_size < 0)
		return false;

	assert(payload_size <= CRDB_RECORD_STREAM_BUF_LEN);
	*generation = buf.header.generation;
	memcpy(dst, buf.data, payload_size);
	*len = (size_t)payload_size;
	return true;
}

#ifdef HAS_PROTOBUF_C
void *
crdb_record_stream_iterator_next_msg(struct crdb_record_stream_iterator *it,
    uint32_t *generation, const ProtobufCMessageDescriptor *descriptor,
    ProtobufCAllocator *allocator)
{
	struct read_record buf;
	ProtobufCMessage *ret = NULL;

	*generation = 0;

	/* We may fail to parse a buffer; keep scanning if that happens. */
	while (ret == NULL) {
		ssize_t payload_size;

		payload_size = record_stream_iterator_next(it, &buf);
		if (payload_size < 0)
			return NULL;

		assert(payload_size <= CRDB_RECORD_STREAM_BUF_LEN);
		ret = protobuf_c_message_unpack(descriptor, allocator,
		    payload_size, buf.data);
	}

	*generation = buf.header.generation;
	return ret;
}
#endif /* HAS_PROTOBUF_C */
