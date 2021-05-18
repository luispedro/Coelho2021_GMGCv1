from jug import CachedFunction, TaskGenerator
from jug.utils import identity
from glob import glob

files = CachedFunction(glob, 'outputs.txt/*.unique.txt.gz')
files.sort()


@TaskGenerator
def nr_uniquely_mapped_reads(fname):
    import pandas as pd
    f = pd.read_table(fname, index_col=0, squeeze=True)
    return f.sum()

results = {f:nr_uniquely_mapped_reads(f) for f in files}
results = identity(results)
