import json
import sys

import pandas as pd
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


def format_schema(schema):
    return f.schema.to_arrow_schema()


def format_metadata(metadata):
    return f.metadata
