from .bow import IterableParquetFile, format_metadata, format_schema
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
    "file",
    type=str,
)
@click.option(
    "-s", "--schema",
    help="Print table schema.",
    is_flag=True
)
def info(file, schema):
    f = IterableParquetFile(file)
    if schema:
        print(format_schema(f.schema))
    else:
        print(format_metadata(f.metadata))


@cli.command()
@click.argument(
    "file",
    type=str,
)
@click.option(
    "-s", "--sep",
    type=str,
    default="\t",
    help="Output field separator [Default: tab]. "
)
@click.option(
    "-H", "--header",
    is_flag=True,
    help="Include column name header in output."
)
@click.option(
    "-i", "--index",
    is_flag=True,
    help="Include index in output."
)
def cat(file, sep, header, index):
    f = IterableParquetFile(file)
    n = 0
    for chunk in f:
        print(chunk.to_csv(sep=sep, index=index, header=header))
        n += len(chunk)


cli()
