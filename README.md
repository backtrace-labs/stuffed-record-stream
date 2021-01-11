% record_stream: a resilient self-synchronising format for log records

[Backtrace](https://www.backtrace.io)'s log record framing format
=================================================================

We use this self-synchronising format to store key metadata for our
server-side embedded crash database.  Operationally, the format has
proven itself resilient, flexible, and easy to tweak with ad hoc
scripts.  We are standardising on this format for persistent logs of
variable-length records.

This [blog post](https://engineering.backtrace.io/2021-01-11-stuff-your-logs/)
contains background and explains why you might to use such a format.
There is also a copy of the post in `doc/2021-01-11-stuff-your-logs.md`.

We released the source under the MIT license, but the scheme is simple
enough that mining the repository for ideas and coding an interface
tuned to one's specific needs might also make sense.
