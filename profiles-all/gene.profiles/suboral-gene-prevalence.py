from scipy import stats
import numpy as np
from matplotlib import use
use('Agg')
from matplotlib import pyplot as plt
from collections import Counter, defaultdict
import pandas as pd
import subsamplex
from glob import glob

suboral = pd.read_table('cold/suboral.tsv', squeeze=True, index_col=0)

SUBSAMPLE = True

counts = defaultdict(Counter)
n = 0
t = len(suboral)

for f,so in suboral.short.iteritems():
    f = pd.read_table(f'outputs.txt/{f}.unique.txt.gz', index_col=0, squeeze=True)
    if f.sum() < 1_000_000:
        print(f'Skip...')
        continue
    if SUBSAMPLE:
        f.values.flat[:] = subsamplex.subsample(f.values.ravel(), 1000*1000)
        f = f[f>0]
    counts[so].update(f.index)
    n += 1
    print(f'Done {n} of {t}')



recounts = pd.DataFrame({k:pd.Series(v) for k, v in counts.items()})
recounts.fillna(0, inplace=True)

oral = recounts.sum(1)
oralcounts = oral.value_counts()

hists = {c:recounts[c].value_counts() for c in recounts.columns}

fig, ax = plt.subplots()
ps = np.arange(1,51)
for k in hists:
    v = hists[k]
    if len(v) < 50:
        print(f'Not enough data for {k}')
        continue
    v = v.loc[ps].fillna(0)
   
    ax.plot(np.arange(1,51), v, '-o', label=k)
    
    
    
ax.set_yscale('log')
ax.set_xscale('log')
ax.legend(loc='best')
if SUBSAMPLE:
    fig.savefig('plots/suboral.1m.svg')
    fig.savefig('plots/suboral.1m.png')
else:
    fig.savefig('plots/suboral.svg')
    fig.savefig('plots/suboral.png')

fig.savefig('plots/suboral.svg')
fig.savefig('plots/suboral.png')

