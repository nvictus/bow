# bow

A simple CLI Parquet file reader (parquet -> csv) and writer (csv -> parquet) using `pyarrow`. (WIP!)

_Most importantly, it works in chunks (row groups) so you can actually stream and write big files._

I don't understand why such a thing doesn't exist yet, so here you go.

No, I'm not going to use the Java `parquet-mr` CLI. Go hadoop yourself.

Maybe I'll include some arrow streaming functionality at some point.


```
Usage: bow [OPTIONS] COMMAND [ARGS]...

Options:
  -V, --version  Show the version and exit.
  -h, --help     Show this message and exit.

Commands:
  info     Print Parquet file metadata.
  par2txt  Convert Parquet to CSV text.
  txt2par  Convert CSV text to Parquet.
```
