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

#include "word_stuff.h"

#include <assert.h>
#include <limits.h>
#include <string.h>

#define RADIX 0xFDUL

/*
 * We encode the first run size with a single byte, in radix 0xFD, to
 * guarantee that writing the size will never introduce a byte
 * sequence that matches the 2-byte header.
 */
#define MAX_INITIAL_RUN (RADIX - 1)

/*
 * We encode every run after the first one with two bytes, in radix 0xFD:
 * a larger radix reduces the asymptotic overhead, and, again, we want to
 * make sure we never introduce a byte sequence that matches the header.
 */
#define MAX_REMAINING_RUN ((RADIX * RADIX) - 1)

/* Hardcode a reasonable cache line size. */
#define CACHE_LINE_SIZE 64

#define CRDB_LIKELY(X) (__builtin_expect(!!(X), 1))
#define CRDB_UNLIKELY(X) (__builtin_expect(!!(X), 0))

#define min(a, b)                                         \
        ({                                                \
                __auto_type _a = (a);                     \
                __auto_type _b = (b);                     \
                _a < _b ? _a : _b;                        \
        })

static const uint8_t header[] = { RADIX + 1, RADIX };

static_assert(sizeof(header) == CRDB_WORD_STUFF_HEADER_SIZE,
    "Header byte sequence does not match the header size.");

inline const uint8_t *
crdb_word_stuff_header_find(const uint8_t *data, size_t num)
{
	const union {
		uint8_t bytes[CRDB_WORD_STUFF_HEADER_SIZE];
		uint16_t word;
	} needle = {
		.bytes = { header[0], header[1] },
	};
	/* Number of bytes where we might find the initial header byte. */
	size_t num_prefix;

	static_assert(sizeof(needle.word) == sizeof(needle.bytes),
	    "Header must be exactly a uint16_t.");

	if (num < CRDB_WORD_STUFF_HEADER_SIZE)
		return data + num;

	num_prefix = num - 1;
	for (size_t i = 0; i < num_prefix; i++) {
		uint16_t actual;

		memcpy(&actual, &data[i], sizeof(actual));
		if (actual == needle.word)
			return data + i;
	}

	return data + num;
}

size_t
crdb_word_stuffed_size(size_t in_size, bool with_header)
{
	size_t static_estimate = CRDB_WORD_STUFFED_BOUND(in_size);
	size_t ret = in_size;

	/*
	 * We can handle much larger inputs, but the input is already
	 * unrealistically large at this point.
	 */
	if (in_size > SSIZE_MAX)
		return SIZE_MAX;

	/* Count the overhead for the initial header + one-byte chunk header */
	ret += (with_header) ? sizeof(header) + 1 : 1;
	if (in_size < MAX_INITIAL_RUN) {
		assert(ret <= static_estimate);
		return ret;
	}

	in_size -= MAX_INITIAL_RUN;
	/*
	 * Add one 2-byte header for each remaining chunk, including
	 * the last partial / empty one.
	 */
	ret += sizeof(uint16_t) * (1 + (in_size / MAX_REMAINING_RUN));
	assert(ret <= static_estimate);
	return ret;
}

inline uint8_t *
crdb_word_stuff_header(uint8_t dst[static CRDB_WORD_STUFF_HEADER_SIZE])
{

	memcpy(dst, header, CRDB_WORD_STUFF_HEADER_SIZE);
	return dst + CRDB_WORD_STUFF_HEADER_SIZE;
}

/*
 * We expect a lot of short copies.  Avoid the overhead of full memcpy
 * for those...
 *
 * We simplify the code by relying on unaligned loads and stores being
 * fast on x86.  Instead of, e.g., implementing a 15-byte copy by
 * copying 8 bytes into &dst[0], and another 7 into &dst[8], we
 * instead copy 8 bytes into &dst[0] and 8 again into &dst[7].
 * There's a bit of duplicated work, but that's much better than
 * splitting the 7-byte copy into multiple smaller writes.
 *
 * On my puny intel, this brings down the decoding time for records <
 * 64 bytes down to ~8 cycles, compared to ~20-30 cycles with a full
 * call to memcpy.
 */
static void
short_memcpy(uint8_t *dst, const uint8_t *src, size_t n)
{

#define MEMCPY(DST, SRC, N) do {				\
		memcpy(DST, SRC, N);				\
		/* Hafta disable memcpy conversion. */		\
		asm volatile("" ::: "memory");			\
	} while (0)

	/*
	 * If we could rely on short rep movsb being fast, we would
	 * just always use that.  However, we can't, especially not on
	 * AMD, so defer to memcpy for large copies, and use the code
	 * below for short (< cache line) ones.
	 */
	if (CRDB_LIKELY(n >= 8)) {
		size_t tail = n - 8;

		if (CRDB_UNLIKELY(n >= CACHE_LINE_SIZE)) {
			memcpy(dst, src, n);
			return;
		}

		/* We'll handle the last 8 bytes with an overlapping write. */
		for (size_t i = 0; i < tail; i += 8)
			MEMCPY(dst + i, src + i, 8);

		MEMCPY(dst + tail, src + tail, 8);
		return;
	}

	/* 4 <= n <= 8: use two potentially overlapping 4-byte writes. */
	if (CRDB_LIKELY(n >= 4)) {
		size_t tail = n - 4;

		MEMCPY(dst, src, 4);
		MEMCPY(dst + tail, src + tail, 4);
		return;
	}

	/* n <= 3.  Decode the length in binary (2 and 1 -byte copies). */
	if (n & 2)
		MEMCPY(dst, src, 2);

	if (n & 1)
		MEMCPY(dst + (n - 1), src + (n - 1), 1);

#undef MEMCPY
	return;
}

static inline uint8_t *
encode_run_size(uint8_t *dst, size_t distance)
{

	assert(distance <= MAX_REMAINING_RUN);

	/* Encode the distance in little-endian with RADIX base. */
	dst[0] = distance % RADIX;
	dst[1] = distance / RADIX;
	return dst + 2;
}

static inline size_t
decode_run_size(const uint8_t src[static 2])
{

	/* The chunk size is encoded in 2 little-endian base RADIX bytes. */
	return src[0] + RADIX * src[1];
}

#define CONSUME(X) do {				\
		size_t consumed_ = (X);		\
						\
		src += consumed_;		\
		src_size -= consumed_;		\
	} while (0)

uint8_t *
crdb_word_stuff_encode(uint8_t *dst, const void *vsrc, size_t src_size)
{
	const uint8_t *src = vsrc;
	uint8_t *ret = dst;
	bool first_header = true;

	/*
	 * Encoding looks for the next forbidden (header) sequence
	 * within the length that can be encoded in the current chunk:
	 * the first chunk uses a single byte, so the maximum length
	 * is MAX_INITIAL_RUN (252), and all other chunks can use two
	 * bytes, so their maximum length is MAX_REMAINING_RUN (252**2).
	 *
	 * In both cases, a value less than the maximum run size denotes
	 * the length of a literal run, followed by an implicit forbidden
	 * header, which can thus be consumed from the source.  Otherwise,
	 * the length denotes a literal run, with no trailing header, so
	 * we only consume the literals copied to the destination.
	 *
	 * We also pretend we appended a forbidden sequence at the end
	 * of the source: that's the only way to ensure we can
	 * represent a short message.  Decoding will strip that final
	 * forbidden sequence.
	 */
	for (;;) {
		const uint8_t *next_forbidden;
		size_t max_run_size;
		size_t run_size;

		if (first_header) {
			max_run_size = MAX_INITIAL_RUN;
			next_forbidden = crdb_word_stuff_header_find(src,
			    min(max_run_size, src_size));
			run_size = next_forbidden - src;

			assert(run_size <= MAX_INITIAL_RUN);
			*ret = run_size;
			ret++;
			first_header = false;
		} else {
			max_run_size = MAX_REMAINING_RUN;
			next_forbidden = crdb_word_stuff_header_find(src,
			    min(max_run_size, src_size));

			run_size = next_forbidden - src;
			ret = encode_run_size(ret, run_size);
		}

		short_memcpy(ret, src, run_size);
		ret += run_size;

		CONSUME(run_size);
		/*
		 * Values less than the chunk size limit are
		 * implicitly suffixed with the stuff byte sequence.
		 */
		if (run_size < max_run_size) {
			/*
			 * We reached the end (with a virtual
			 * terminating forbidden byte sequence).
			 */
			if (src_size == 0)
				break;

			assert(src_size >= CRDB_WORD_STUFF_HEADER_SIZE &&
			    src[0] == header[0] && src[1] == header[1] &&
			    "If we stopped short, we must have found "
			    "a forbidden header word.");
			CONSUME(CRDB_WORD_STUFF_HEADER_SIZE);
		}
	}

	return ret;
}

uint8_t *
crdb_word_stuff_decode(uint8_t *dst, const void *vsrc, size_t src_size)
{
	const uint8_t *src = vsrc;
	uint8_t *ret = dst;
	bool first_header = true;

	/*
	 * This is the inverse of the encode loop, with additional
	 * logic to avoid out-of-bound reads and detect obviously bad
	 * data.
	 *
	 * When we read the first header, we know it fits in one byte,
	 * and must be at most MAX_INITIAL_RUN.  After that, we read
	 * 2-byte headers, which must represent a length of at most
	 * MAX_REMAINING_RUN.
	 *
	 * When the length is less than the maximum value, it denotes
	 * a run of literals (of the encoded length) followed by the
	 * forbidden (header) 2-byte sequence.  When the length is
	 * equal to the maximum value, it denotes a run of literals,
	 * without any implicit trailing sequence.
	 *
	 * Once we arrive at the last chunk, we should find a chunk
	 * that encodes a literal followed by a header.  That last
	 * header was virtually appended to the actual message during
	 * encoding, and we do not want to actually write it to the
	 * destination: it simply tells us that we correctly reached
	 * the end of the message.
	 */
	for (;;) {
		size_t max_run_size;
		size_t run_size;

		if (first_header) {
			max_run_size = MAX_INITIAL_RUN;

			if (CRDB_UNLIKELY(src_size < 1))
				return NULL;

			run_size = src[0];
			CONSUME(1);
			first_header = false;
		} else {
			max_run_size = MAX_REMAINING_RUN;

			if (CRDB_UNLIKELY(
			    src_size < CRDB_WORD_STUFF_HEADER_SIZE))
				return NULL;

			run_size = decode_run_size(src);
			CONSUME(CRDB_WORD_STUFF_HEADER_SIZE);
		}

		if (CRDB_UNLIKELY(src_size < run_size ||
		    run_size > max_run_size))
			return NULL;

		short_memcpy(ret, src, run_size);
		ret += run_size;
		CONSUME(run_size);

		/* We have to add the implicit header. */
		if (run_size < max_run_size) {
			/* Unless it's the virtual terminating header. */
			if (src_size == 0)
				break;

			/*
			 * If it's not the end, there must at least be
			 * a header remaining.  We check here before
			 * writing to dst to preserve the invariant
			 * that we'll never write more bytes than the
			 * initial src_size - 1.
			 */
			if (CRDB_UNLIKELY(
			    src_size < CRDB_WORD_STUFF_HEADER_SIZE))
				return NULL;

			ret = crdb_word_stuff_header(ret);
		}
	}

	return ret;
}

#undef CONSUME
