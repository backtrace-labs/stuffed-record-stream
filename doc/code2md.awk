#!/usr/bin/awk -f

# This scripts converts a source file to a markdown document, line by
# line.
#
# Markdown sections are marked with
# /**
#  ** arbitrary markdown
#  ## ...
#  **/
#
# Such sections may be terminated with a folding block annotation:
# **{[optional description]
# **/
#
# If so, the following code block will be hidden in an expandable
# <details> element.
#
# The script also supports ocaml/pascal -style block comments,
# (**
#  ** more markdown
#  **)
#
# and C++/Java
#
#  /// markdown
#
# Lua
#
#  --- markdown
#
# or Lisp / assembly
#
#  ;;; markdown
#
# blocks.
#
# No attempt is made to match or restrict markdown comment syntax.
# Don't write weird code, or, if you do, indent it.

BEGIN {
	code_prefix = "\t "

	# The details_pos is initially undecided.  After that, it is either
	# "details" or "toplevel": in "details" details_pos, we are inside a
	# <details> element.
	#
	# If the first thing we see is code, we start an implicit
	# details block; otherwise, they must be explicitly enabled in
	# text blocks.
	details_pos = "undecided"

	# The last line is "code" if we printed the last line with the
	# code prefix, and "text" otherwise.
	last_line = "undecided"

	# We use /** to start markdown blocks, same as doxygen: we
	# have to remember when we scan such a line and only print it
	# if the next line is code.
	last_header = ""

	# We assume an external program has already scanned the file
	# to find the first toplevel header; we thus don't want to
	# print that first header.
	skip_first_header = 1
}

function print_code(str)
{
	if (details_pos == "undecided") {
		print "<details><summary>Headers</summary>"
		details_pos = "details"
		last_line = "text"
	}

	if (last_line == "text")  # Ensure an empty line when switching to code
		print ""
	if (last_header != "")
		print code_prefix last_header

	print code_prefix str
	last_line = "code"
	last_header = ""
}

function print_text(str)
{
	if (details_pos == "undecided")
		details_pos = "toplevel"
	# Insert an empty line after a code block, and close any
	# details block in that transition.
	if (last_line == "code") {
		print ""
		if (details_pos == "details")
			print("</details>\n")
		details_pos = "toplevel"
	}

	print str
	last_line = "text"
	last_header = ""
}

# Buffer this line: it's either the start of a comment block (don't
# print), or a doxygen comment (do print).
/^[\/(]\*\*$/ {
	last_header = $0
	next
}

# Detect the first header (which we must not print).
/^ (\*\*|##|\/\/\/|---|;;;) #[^#]/ && skip_first_header {
	skip_first_header = 0
	sub(/#.*$/, "", $0)
}

# An anonymous block is opened with " **{"
/^ (\*\*|##|\/\/\/|---|;;;){ *$/{
	print_text("\n<details>\n")
	details_pos = "details"
	next
}

# We can also have a details block with a summary
/^ (\*\*|##|\/\/\/|---|;;;){/{
	print_text("\n<details><summary>")
	sub(/^ \*\*{ ?/, "", $0)
	print_text($0)
	print_text("</summary>\n")

	details_pos = "details"
	next
}

# This is a text line.
/^ (\*\*|##|\/\/\/|---|;;;)( |$)/ {
	sub(/^ \*\* ?/, "", $0)
	print_text($0)
	next
}

# Print a closing comment as an empty text line.
/^ \*\*[\/)]$/ {
	print_text("")
	next
}

# And otherwise, we have a code line.
{
	print_code($0)
}

END {
	if (last_header != "")
		print_code("")
	if (details_pos == "details")
		print("\n</details>")
}
