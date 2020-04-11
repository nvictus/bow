# bow

A simple CLI Parquet file reader (parquet -> csv) and writer (csv -> parquet) using `pyarrow`.
Most importantly, it works in chunks (row groups) so you can actually stream and write big files.

I don't understand why such a thing doesn't exist yet, so here you go.

No, I'm not going to use the Java `parquet-mr` CLI. Go hadoop yourself.

Maybe I'll include some arrow streaming functionality at some point.
