from collections import Counter, defaultdict
import pandas as pd
from glob import glob
files = glob('outputs/*.feather')
files.sort()
biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)

counts = defaultdict(Counter)
for i,fname in enumerate(files):
    f = pd.read_feather(fname)
    sample = fname.split('/')[1].split('.')[0]
    seen = f['index'][f['raw_unique'] > 0]
    counts[biome[sample]].update(seen.values)
    if i % 100 == 99:
        print("Done {}/{}".format(i+1, len(files)))

recounts = pd.DataFrame({k:pd.Series(v) for k, v in counts.items()})
recounts.fillna(0, inplace=True)
used_total = recounts.sum(1)
recounts['all'] = used_total
recounts = recounts.astype(int)
recounts.reset_index(inplace=True)
recounts.to_feather('tables/genes.unique.prevalence.feather')

names = [line.strip() for line in open('cold/derived/GMGC10.headers')]
recounts.set_index('index', inplace=True)
recounts.index = recounts.index.map(names.__getitem__)
recounts.to_csv('tables/genes.unique.prevalence.txt', sep='\t')
