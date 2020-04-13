import json
import sys
import pandas as pd
from pandas.io.common import get_filepath_or_buffer

from .bow import IterableParquetFile, format_metadata, format_schema, to_parquet
from ._version import __version__

import click

CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
}


@click.version_option(__version__, "-V", "--version")
@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass


@cli.command()
@click.argument(
    "path", 
    type=str,
)
@click.option(
    "-s", "--schema", 
    help="Print table schema.", 
    is_flag=True,
)
def info(path, schema):
    """
    Print Parquet file metadata.

    """
    path, _, _, should_close = get_filepath_or_buffer(path)

    f = IterableParquetFile(path)
    if schema:
        print(format_schema(f.schema))
    else:
        print(format_metadata(f.metadata))

    if should_close:
        path.close()


@cli.command()
@click.argument(
    "path", 
    type=str,
)
@click.option(
    "-s", "--sep",
    type=str,
    default="\t",
    help="Output field separator [Default: tab]. ",
)
@click.option(
    "-H", "--header", 
    is_flag=True, 
    help="Include column name header in output.",
)
@click.option(
    "-i", "--index", 
    is_flag=True, 
    help="Include index in output.",
)
def par2txt(path, sep, header, index):
    """
    Convert Parquet to CSV text.

    """
    path, _, _, should_close = get_filepath_or_buffer(path)

    f = IterableParquetFile(path)
    n = 0
    for chunk in f:
        print(chunk.to_csv(sep=sep, index=index, header=header))
        n += len(chunk)

    if should_close:
        path.close()


@cli.command()
@click.argument(
    "path", 
    type=str,
)
@click.argument(
    "outpath", 
    type=str,
)
@click.option(
    "-ic", "--input-chunksize",
    type=int,
    default=4096,
    help="Input buffer size in bytes",
)
@click.option(
    "-oc", "--output-chunksize",
    type=int,
    default=1000000,
    help="Output row group size (# of rows)",
)
@click.option(
    "-s", "--sep",
    type=str,
    default="\t",
    help="Output field separator [Default: tab]. ",
)
@click.option(
    "-H", "--header", 
    is_flag=True, 
    help="Input file contains a header.",
)
@click.option(
    "-i", "--index", 
    is_flag=True, 
    help="Set one of the input columns to be the index.",
)
@click.option(
    "-k", "--schema", 
    type=str, 
    help="Provide data types for columns as a JSON string.",
)
def txt2par(
    path, outpath, sep, header, index, schema, input_chunksize, output_chunksize
):
    """
    Convert CSV text to Parquet.

    """
    if path == "-":
        path = sys.stdin

    path, _, _, _ = get_filepath_or_buffer(path)

    if schema is not None:
        schema = json.loads(schema)

    if header:
        header = "infer"
    else:
        header = None

    it = pd.read_csv(
        path,
        sep=sep,
        header=header,
        dtype=schema,
        iterator=True,
        chunksize=input_chunksize,
    )

    to_parquet(it, outpath, row_group_size=output_chunksize, compression="snappy")
