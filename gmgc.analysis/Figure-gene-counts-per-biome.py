import matplotlib
matplotlib.use('Agg')

USE_1M = True
USE_COMPLETE_ONLY = False

import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
from matplotlib import style


from colors import biome2color
style.use('default')
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)

N = 100
XTICKS = [1,2,4,10,20,50,100, 200, 500]

biome2color['all'] = '#aaaaaa'

n_biome = {'human gut': 5319,
 'built-environment': 592,
 'pig gut': 295,
 'human oral': 226,
 'soil': 214,
 'mouse gut': 185,
 'marine': 130,
 'dog gut': 129,
 'cat gut': 124,
 'amplicon': 105,
 'human skin': 51,
 'freshwater': 49,
 'isolate': 16,
 'wastewater': 15,
 'human vagina': 6,
 'human nose': 2,
 'all': 7458}



def do_plot(hists, ax):
    hists = pd.read_table(hists, index_col=0)
    for b in hists.columns:
        if b in {'amplicon', 'isolate'}: continue
        n = n_biome[b]
        N = min(1000, n//3)
        if n < 100:
            continue
        gp = hists[b].values[1:]

        ax.scatter(np.arange(1,N+1), gp[:N], c=biome2color[b], s=2)
        ax.plot(np.arange(1, N+1), gp[:N], label="{} {}".format(b, n), c=biome2color[b])


HISTSF = [
    'tables/genes.prevalence.1m.no-dups.hists.txt',
    'tables/GMGC.90nr.gene.hists.1m.no-dups.txt',
    'tables/GMGC.gf.gene.hists.1m.no-dup.txt',
    ]

HISTSC = [
    'tables/GMGC10.histogram.1m.no-dups.complete-only.tsv',
    'tables/GMGC10.histogram.1m.no-dups.clusters90.only-w-complete.tsv',
    'tables/gf-histogram-only-genes-in-complete.tsv',
    ]

if USE_COMPLETE_ONLY:
    HISTS = HISTC
else:
    HISTS = HISTSF

fig,axes = plt.subplots(3, 1, sharex=True, figsize=[4, 7])
for h,ax in zip(HISTS, axes):
    do_plot(h, ax)
    #ax.legend(loc='best')
    ax.set_yscale('log')
axes[1].set_yticks([], minor=True)
axes[2].set_xscale('log')
axes[2].set_xticks(XTICKS)
axes[2].set_xticklabels(map(str,XTICKS))
axes[2].set_xticks([], minor=True)
axes[2].set_xlabel('Number of detections')
axes[2].set_xlabel('Gene Counts')
sns.despine(fig, offset=4, trim=True)
fig.tight_layout()

fig.savefig('plots/Figure-rare.3.svg')

