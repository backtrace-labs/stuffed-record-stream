#!/bin/bash
set -e

OUT=${1:-"/tmp/record_stream_docs"}

BASE=$(dirname $(readlink -f "$0"))

CRDB=$(dirname "${BASE}")

CSS="assets/modest.css"

PANDOC_FLAGS="--tab-stop=8 --toc --css $CSS --from markdown+smart --to html5 --standalone"

SOURCES=$(cat <<'EOF'
README.md
doc/2021-01-11-stuff-your-logs.md
include/crdb_error.h
include/record_stream.h
include/word_stuff.h
EOF
)

BEFORE=$(echo $SOURCES | "$BASE/navbar.awk")

INDEX="README.md.html"

COMMENTS_RE='(\*\*|##|///|---|;;;)'

mkdir -p $OUT

echo "Generating documentation to $OUT"

for SOURCE in $SOURCES;
do
    DST="${OUT}/$(echo "$SOURCE" | sed -e 's|/|__|g').html"
    echo "Converting $SOURCE -> $DST"
    if [ ${SOURCE: -3} == ".md" ];
    then
	pandoc $PANDOC_FLAGS --include-before-body <(echo $BEFORE) \
	       --title-prefix="$SOURCE" \
	       -i "${CRDB}/${SOURCE}" -o "$DST";
    else
	TITLE=$(egrep -m 1 -e "^ $COMMENTS_RE # " "${CRDB}/${SOURCE}" | sed -re "s^ $COMMENTS_RE # ^^")
	if [ -z "$TITLE" ];
	then
	    awk '{print "\t" $0}' < "${CRDB}/${SOURCE}"
	else
	    $BASE/code2md.awk < "${CRDB}/${SOURCE}";
	fi | \
	    pandoc $PANDOC_FLAGS --include-before-body <(echo $BEFORE) \
		   --title-prefix="$SOURCE" --metadata="title: $TITLE" \
		   -o "$DST";
    fi
done

cp -r "$BASE/assets" "$OUT/"
cp "$OUT/$INDEX" "$OUT/index.html"
