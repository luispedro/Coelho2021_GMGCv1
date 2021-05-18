import pandas as pd
from glob import glob
from jug import TaskGenerator
biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
files = glob('outputs/*.feather')
files.sort()
samples = [f.split('/')[1].split('.')[0] for f in files]

@TaskGenerator
def sum_up_partial(oname, files):
    total = None
    for f in files:
        f = pd.read_feather(f, nthreads=8)
        f.set_index('index', inplace=True)
        if total is None:
            total = f
        else:
            total = total.add(f, fill_value=0)
        total.fillna(0, inplace=True)
    total.reset_index(inplace=True)
    total.to_feather(oname)
    return oname

@TaskGenerator
def sum_up_biome(b, files):
    oname = 'tables/totals/{}.feather'.format(b.replace(' ', '-'))
    return sum_up_partial.f(oname, files)

def blocks_of(xs, n):
    while xs:
        yield xs[:n]
        xs = xs[n:]

for b in set(biome.values):
    cur = set(biome.index[biome == b])
    curf = [f for f,s in zip(files, samples) if s in cur]
    if len(curf) <= 200:
        sum_up_biome(b, curf)
    else:
        curf = [sum_up_partial('partials/sum_up_{}.{}.feather'.format(b.replace(' ', '-'), i), xs) for i,xs in enumerate(blocks_of(curf, 64))]
        sum_up_biome(b, curf)

