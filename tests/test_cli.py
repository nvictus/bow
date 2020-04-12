from contextlib import redirect_stdout
from io import StringIO
from tempfile import NamedTemporaryFile
import os.path as op

import pandas as pd
import pytest
from bow.__main__ import par2txt, txt2par

testdir = op.realpath(op.dirname(__file__))
datadir = op.join(testdir, "data")
# example.parquet file from parquet-cli (https://github.com/chhantyal/parquet-cli)


def test_par2txt():
    path = op.join(datadir, 'example.parquet')

    df1 = pd.read_parquet(path)
    with StringIO() as buf, redirect_stdout(buf):
        par2txt.callback(path, sep='\t', header=True, index=False)
        buf.seek(0)
        df2 = pd.read_csv(buf, sep='\t')
    
    assert df1['salary'].equals(df2['salary'])


def test_txt2par():
    path = op.join(datadir, 'example.parquet')

    df1 = pd.read_parquet(path)
    with NamedTemporaryFile('w+t') as fin, \
         NamedTemporaryFile('w+b') as fout, \
         redirect_stdout(fin):
        
        par2txt.callback(path, sep='\t', header=True, index=False)
        fin.seek(0)
        
        txt2par.callback(
            path=fin.name, 
            outpath=fout.name, 
            sep="\t", 
            header=True, 
            index=None, 
            schema=None,
            input_chunksize=4096,
            output_chunksize=1000
        )
        fout.seek(0)

        df2 = pd.read_parquet(fout.name)
    
    assert df1['salary'].equals(df2['salary'])
