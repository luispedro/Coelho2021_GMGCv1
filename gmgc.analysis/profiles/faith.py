# IPython log file
from jug import TaskGenerator, CachedFunction
import pandas as pd
from glob import glob
tables = CachedFunction(glob,'/g/bork1/tschmidt/GMGC_phylodiv/results/per_sample/diversity.*.tsv.gz')
tables.sort()
    

@TaskGenerator
def read_tables(ts):
    faith = {}
    for t in ts:
        data = pd.read_table(t, compression=None)
        if data['size.COG525'].max() < 100:
            continue
        f = data[(data['size.COG525'] == 100) & (data['type'] == 'Faith_PD')]['div.median']
        sample = t.split('/')[-1].split('.')[1]
        faith[sample] = f.values[0]
    return faith

def blocks_of(xs, n):
    while xs:
        yield xs[:n]
        xs = xs[n:]

@TaskGenerator
def save_faith(data):
    faith = pd.concat([pd.Series(d, name='FaithPD') for d in data])
    pd.DataFrame({'FaithPD': faith}).to_csv('tables/faithPD.tsv', sep='\t')


data = []
for ts in blocks_of(tables, 64):
    data.append(read_tables(ts))

save_faith(data)
