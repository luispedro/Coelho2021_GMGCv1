from collections import Counter, defaultdict
import pandas as pd
from glob import glob
import subsamplex

files = glob('outputs.txt/*.unique.txt.gz')
files.sort()
biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
duplicates = set(line.strip() for line in open('cold/duplicates.txt'))

counts = defaultdict(Counter)
skipped = 0
for i,fname in enumerate(files):
    sample = fname.split('/')[1].split('.')[0]
    if sample in duplicates:
        skipped += 1
        if skipped % 100 == 99:
            print(f'Skip {skipped}')
        continue
    f = pd.read_table(fname, index_col=0, squeeze=True)
    if f.sum() < 1_000_000:
        skipped += 1
        if skipped % 100 == 99:
            print(f'Skip {skipped}')
        continue
    f.values.flat[:] = subsamplex.subsample(f.values.ravel(), 1000*1000)
    f = f[f>0]
    counts[biome[sample]].update(f.index)
    if i % 100 == 99:
        print("Done {}/{}".format(i+1, len(files)))

recounts = pd.DataFrame({k:pd.Series(v) for k, v in counts.items()})
recounts.fillna(0, inplace=True)
used_total = recounts.sum(1)
recounts['all'] = used_total
recounts = recounts.astype(int)
recounts.reset_index(inplace=True)
recounts.to_feather('tables/genes.1m.unique.prevalence.no-dups.feather')

names = [line.strip() for line in open('cold/derived/GMGC10.headers')]
recounts.set_index('index', inplace=True)
recounts.index = recounts.index.map(names.__getitem__)
recounts.to_csv('tables/genes.1m.unique.no-dups.prevalence.txt', sep='\t')

