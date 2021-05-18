import pandas as pd
from glob import glob
from jug import TaskGenerator, CachedFunction

_cogs = None
def load_cogs():
    cogfiles = glob('cold/single.copy.MGs/all-matches/COG*.txt')

    cogs = []
    for c in sorted(cogfiles):
        cur = set()
        for line in open(c):
            cur.add(int(line.strip().split('.')[1], 10))
        cogs.append(frozenset(cur))
    return cogs

@TaskGenerator
def count_gene_cogs(f):
    global _cogs
    if _cogs is None:
        _cogs = load_cogs()
    s = pd.read_feather(f)
    s = s[s['raw'] > 0]
    s = set(s['index'].values)
    return [len(s)] + [len(c&s) for c in _cogs]


@TaskGenerator
def save_counts_table(counts, files, oname):
    samples = [f.split('/')[1].split('.')[0] for f in files]
    csamples = pd.DataFrame(counts, index=samples)
    csamples.to_csv(oname, sep='\t')


files = CachedFunction(glob, 'outputs/*.feather')
files.sort()
counts = [count_gene_cogs(f) for f in files]

save_counts_table(counts, files, 'tables/cogs.counts.txt')
