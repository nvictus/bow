import json
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

pd.set_option("display.max_columns", None)


class IterableParquetFile(pq.ParquetFile):
    def __iter__(self):
        self.__rgid = 0
        return self

    def __next__(self):
        if self.__rgid < self.num_row_groups:
            rg = self.read_row_group(
                self.__rgid,
                columns=self.schema.names,
                use_threads=True,
                use_pandas_metadata=True)
            self.__rgid += 1
        else:
            raise StopIteration

        return rg.to_pandas()


def read_parquet(filepath, columns=None, iterator=False, **kwargs):
   """
   Load DataFrames from Parquet files, optionally in pieces.

   Parameters
   ----------
   filepath : str, pathlib.Path, pyarrow.NativeFile, or file-like object
       Readable source. For passing bytes or buffer-like file containing a
       Parquet file, use pyarorw.BufferReader
   columns: list
       If not None, only these columns will be read from the row groups. A
       column name may be a prefix of a nested field, e.g. 'a' will select
       'a.b', 'a.c', and 'a.d.e'
   iterator : boolean, default False
       Return an iterator object that yields row group DataFrames and
       provides the ParquetFile interface.
   use_threads : boolean, default True
       Perform multi-threaded column reads
   memory_map : boolean, default True
       If the source is a file path, use a memory map to read file, which can
       improve performance in some environments

   Returns
   -------
   DataFrame or ParquetFileIterator

   """
   use_threads = kwargs.pop('use_threads', True)

   if not iterator:
       return pd.read_parquet(filepath, columns=columns,
                              use_threads=use_threads, **kwargs)
   else:
       return IterableParquetFile(filepath, **kwargs)


def to_parquet(pieces, outpath, row_group_size=None, compression='snappy',
              use_dictionary=True, version=None, **kwargs):
   """
   Save an iterable of dataframe chunks to a single Apache Parquet file. For
   more info about Parquet, see https://arrow.apache.org/docs/python/parquet.html.

   Parameters
   ----------
   pieces : DataFrame or iterable of DataFrame
       Chunks to write
   outpath : str
       Path to output file
   row_group_size : int
       Number of rows per row group
   compression : {'snappy', 'gzip', 'brotli', 'none'}, optional
       Compression algorithm. Can be set on a per-column basis with a
       dictionary of column names to compression lib.
   use_dictionary : bool, optional
       Use dictionary encoding. Can be set on a per-column basis with a list
       of column names.

   See also
   --------
   pyarrow.parquet.write_table
   pyarrow.parquet.ParquetFile
   fastparquet

   """
   if isinstance(pieces, pd.DataFrame):
       pieces = (pieces,)

   try:
       for i, piece in enumerate(pieces):
           table = pa.Table.from_pandas(piece, preserve_index=False)
           if i == 0:
               writer = pa.parquet.ParquetWriter(
                   outpath,
                   table.schema,
                   compression=compression,
                   use_dictionary=use_dictionary,
                   version=version,
                   **kwargs)
           writer.write_table(table, row_group_size=row_group_size)
   finally:
       writer.close()


def format_schema(schema):
    return schema.to_arrow_schema()


def format_metadata(metadata):
    return metadata
