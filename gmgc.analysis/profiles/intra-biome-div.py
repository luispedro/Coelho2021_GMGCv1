import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import seaborn as sns
from scipy.spatial import distance
import numpy as np
import pandas as pd

bactNOG = pd.read_feather('tables/old-functional/bactNOG.scaled.feather')
bactNOG.set_index('index', inplace=True)
mapped = pd.read_table('tables/genes.total.txt', squeeze=True, index_col=0)
normed = bactNOG.T/ mapped.reindex(index=bactNOG.index)
biome = pd.read_table('../../gmgc.analysis/cold/biome.txt', squeeze=True, index_col=0)

normed = normed[normed.mean(1) > 1e-4]
normed = normed.T[(mapped >= 1e6)]
normed10 = np.log10(1e-6 + normed)
biome = biome.reindex(normed.index)

dists = []
nsamples = {}
for b in set(biome.values):
   c = normed10[biome == b]
   dists.extend((b,d) for d in distance.pdist(c.values))
   nsamples[b] = len(c)
dists = pd.DataFrame(dists, columns=['biome', 'dist'])

mdist = dists.dist.groupby(dists.biome).mean()
mdist.sort_values(inplace=True)

fig,ax = plt.subplots()
sns.boxplot(x='biome', y='dist', data=dists, order=mdist.index, ax=ax)
sns.despine(fig, offset=True)
for x in ax.get_xticklabels():
    x.set_rotation(90)
fig.tight_layout()
fig.savefig("plots/intra-biome-bactNOG-div.png")
