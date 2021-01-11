#!/usr/bin/awk -f

BEGIN {
    RS = " "
    print "<div class='sidenav'>"
    prev = ""
}

{
    gsub(/\n/, "")

    # Split on slashes, and find the common prefix.
    split(prev, prev_splits, "/")
    split($0, curr_splits, "/")

    for (mismatch = 1; (mismatch in prev_splits) && (mismatch in curr_splits); mismatch++) {
	if (prev_splits[mismatch] != curr_splits[mismatch])
	    break;
    }

    # Save the current line for the next.
    prev = $0

    # Now reconstruct the common prefix
    common = ""
    for (i = 1; i < mismatch; i++)
	common = common (common == "" ? "" : "/") curr_splits[i]

    # Then the new suffix
    suffix = ""
    for (; i in curr_splits; i++)
	suffix = suffix (suffix == "" ? "" : "/") curr_splits[i]

    # Convert the file's path to the html file.
    target = $0
    gsub(/\//, "__", target)
    target = target ".html"
    print ("<samp>"							\
	   "<span class='sidenav-common'>" common "</span>"		\
	   (common == "" ? "" : "/")					\
	   "<a class='sidenav-suffix' href='" target "'>" suffix "</a>"	\
	   "</samp><br />")
}

END {
    print "</div>"
}
