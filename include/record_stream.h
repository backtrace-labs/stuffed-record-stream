#pragma once

/**
 * A record stream is a corruption-resilient stream of (protobuf)
 * serialized messages.  A stream supports two operations:
 *
 * 1. Append a serialized record to a file descriptor.
 * 2. Iterate through all the valid records in a file descriptor.
 */

#ifdef HAS_PROTOBUF_C
#include <protobuf-c/protobuf-c.h>
#endif /* HAS_PROTOBUF_C */
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "crdb_error.h"

/**
 * We only support up to 512 raw bytes of payload on writes, and allow
 * up to 1024 encoded bytes on reads.  The read limit
 * CRDB_RECORD_STREAM_BUF_LEN must be at least as high as
 * CRDB_WORD_STUFFED_BOUND(CRDB_RECORD_STREAM_MAX_LEN + [internal header]).
 *
 * The current use case for record stream only needs ~20-40 byte
 * records.  These limits are more than generous enough, while still
 * being reasonable for stack allocation.  The read limit has twice as
 * much headroom as the write limit to help forward compatibility:
 * future versions may be able to write backward compatible protos
 * that include many more fields than the initial schema.
 */
enum {
	CRDB_RECORD_STREAM_MAX_LEN = 512,
	CRDB_RECORD_STREAM_BUF_LEN = 2 * CRDB_RECORD_STREAM_MAX_LEN,
};

struct crdb_record_stream_iterator {
	const uint8_t *cursor;
	const uint8_t *end;
	/*
	 * Stop returning records as soon as their first byte
	 * (including the header) is at or after `stop_at`.
	 *
	 * This is initially equal to `end`, but can be shifted
	 * earlier in the record stream.  Note that a record that
	 * starts before `stop_at` will end after that pointer.
	 */
	const uint8_t *stop_at;

	const uint8_t *begin;

	/* The following is only populated when iterating over fds. */
	const uint8_t *header;
	void *mapped;
	size_t map_size;

	bool first_record;

	/* Everything in `mapped` before first_nonzero is zero-filled bytes. */
	const uint8_t *first_nonzero;
};

/**
 * Ensures the contents of `fd` are ready to append more records.
 *
 * This function should be called before appending data to a `fd` that
 * might contain corrupt data.  A call is useless when writing to a
 * fresh empty file, but never harmful.
 *
 * @param fd a file descriptor opened with O_APPEND; may be repositioned.
 */
bool crdb_record_stream_append_initial(int fd, crdb_error_t *);

/**
 * Ensures the contents of `stream` are ready to append more records.
 *
 * This function should be called before appending data to a stream
 * that might contain corrupt data.  A call is useless when writing to
 * a fresh empty file, but never harmful.
 */
bool crdb_record_stream_write_initial(FILE *stream, crdb_error_t *);

/**
 * Appends a record containing `buf[0 ... len - 1]` to `fd`.
 *
 * @param fd a file descriptor opened with O_APPEND.
 */
bool crdb_record_stream_append_buf(int fd, uint32_t generation,
    const uint8_t *buf, size_t len, crdb_error_t *);

/**
 * Writes a record containing `buf[0 ... len - 1]` to `stream`.
 *
 * This function never attempts to handle errors internally and
 * should only be used to write to private temporary files.
 */
bool crdb_record_stream_write_buf(FILE *stream, uint32_t generation,
    const uint8_t *buf, size_t len, crdb_error_t *);

#ifdef HAS_PROTOBUF_C
/**
 * Serializes `message` and appends that record to `fd`.
 *
 * @param fd a file descriptor opened with O_APPEND.
 */
bool crdb_record_stream_append_msg(int fd, uint32_t generation,
    const ProtobufCMessage *message, crdb_error_t *);

/**
 * Serializes `message` and writes that record to `stream`.
 *
 * This function never attempts to handle errors internally and
 * should only be used to write to private temporary files.
 */
bool crdb_record_stream_write_msg(FILE *stream, uint32_t generation,
    const ProtobufCMessage *message, crdb_error_t *);
#endif /* HAS_PROTOBUF_C */

/**
 * Initializes an iterator to scan for records in `buf[0 ... size - 1]`.
 */
void crdb_record_stream_iterator_init_buf(struct crdb_record_stream_iterator *,
    const uint8_t *buf, size_t size);

/**
 * Initializes an iterator to scan for records in `fd`.
 *
 * @param fd a descriptor for a mmap-able file.  May be repositioned (lseek'ed).
 */
bool crdb_record_stream_iterator_init_fd(struct crdb_record_stream_iterator *,
    int fd, crdb_error_t *);

/**
 * Deinitializes an iterator.
 */
void crdb_record_stream_iterator_deinit(struct crdb_record_stream_iterator *);

/**
 * Returns the number of bytes in the record stream.
 */
size_t crdb_record_stream_iterator_size(const struct crdb_record_stream_iterator *);

/**
 * Sets the record stream to start looking for valid records at `start_offset`.
 *
 * No-ops on error.
 *
 * @return false if that points the iterator at a range of clearly invalid data.
 */
bool crdb_record_stream_iterator_locate_at(struct crdb_record_stream_iterator *,
    size_t start_offset);

/**
 * Sets the stop offset for a record stream iterator.
 *
 * Once set, the iterator will successfully stop yielding new records
 * instead of yielding a record starting at or after `stop_offset`.
 *
 * Paired with `locate_at`, this lets us partition a record stream in
 * non-overlapping half-open ranges, by the records' first bytes.
 */
void crdb_record_stream_iterator_stop_at(struct crdb_record_stream_iterator *,
    size_t stop_offset);

/**
 * Decodes and consumes the next valid record in the iterator.
 *
 * @param generation populated with the record's generation on success, 0 on failure.
 * @param dst overwritten with the record's contents.
 * @param len populated with the payload size on success, 0 on failure.
 *
 * @return true if a valid record was found, false on EOF.
 */
bool crdb_record_stream_iterator_next_buf(struct crdb_record_stream_iterator *,
    uint32_t *generation, uint8_t dst[static CRDB_RECORD_STREAM_BUF_LEN],
    size_t *len);

#ifdef HAS_PROTOBUF_C
/**
 * Deserializes and returns the next valid protobuf message.
 *
 * @param generation populated with the record's generation on success, 0 on failure.
 * @param descriptor the protobuf-c descriptor for the message type to decode.
 * @param allocator the allocator used to build the return value, or NULL for
 *    the default allocator.
 *
 * @return a valid ProtobufCMessage for `descriptor`, or NULL on EOF.
 */
void *crdb_record_stream_iterator_next_msg(
    struct crdb_record_stream_iterator *, uint32_t *generation,
    const ProtobufCMessageDescriptor *descriptor,
    ProtobufCAllocator *allocator);
#endif /* HAS_PROTOBUF_C */
