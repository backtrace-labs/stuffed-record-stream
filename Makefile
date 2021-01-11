CFLAGS := -std=gnu11 -Iinclude/ -msse4.2 -O2 -g -fPIC

# Optionally build with protobuf-c convenience wrappers
# CFLAGS += -DHAS_PROTOBUF_C

.PHONY: all doc clean
all: librecord_stream.a

librecord_stream.a: src/record_stream.o src/word_stuff.o
	ar r $@ $^
	ranlib $@

doc:
	doc/generate_html_doc.sh generated_html

clean:
	rm -f librecord_stream.a
	rm -f src/*.o
	rm -rf generated_html

src/record_stream.o: include/record_stream.h include/word_stuff.h include/crdb_error.h
src/word_stuff.o: include/word_stuff.h
