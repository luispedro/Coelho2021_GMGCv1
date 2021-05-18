%matplotlib qt
from scipy import stats
from sklearn import linear_model

import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
from matplotlib import style

style.use('default')

import seaborn as sns
from scipy import stats

import pandas as pd
import numpy as np
from scipy import stats
from colors import biome2color
print("Imported")

up = {
   'cat gut': 'mammal gut',
   'dog gut': 'mammal gut',
   'human blood plasma': 'other human',
   'human gut': 'mammal gut',
   'human nose': 'other human',
   'human oral': 'other human',
   'human skin': 'other human',
   'human vagina': 'other human',
   'mouse gut': 'mammal gut',
   'pig gut': 'mammal gut',
}


biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
upome = biome.map(lambda b: up.get(b,b))
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
total = computed.insertsHQ
divs = pd.read_table('tables/diversity.tsv', index_col=0)
divs['ko_1m_shannon'] = np.exp(divs['ko_1m_shannon'])
#divs['bactNOG_1m_shannon'] = np.exp(divs['bactNOG_1m_shannon'])
f = pd.read_table('tables/faithPD.tsv', index_col=0, squeeze=True)
divs['faithPD'] = f.reindex(divs.index).fillna(f.mean())

riches = pd.read_table('tables/gf.richness.tsv', index_col=0)
divs['gf_1m_rich'] = riches['gf_1m_rich']

print("Loaded basic")
valid = (total > 1e6) & ~biome.map({'isolate', 'amplicon'}.__contains__)
valid &= divs.reindex(valid.index[valid]).gene_1m_rich > 0
valid = valid.index[valid]
total = total.reindex(valid)
biome = biome.reindex(valid)
upome = upome.reindex(valid)
divs = divs.reindex(valid)
pca = pd.read_table('tables/gf.logeuc.pca2.tsv', index_col=0)

fig, axes = plt.subplots(2,2, sharex='col', sharey='row', figsize=[5,5], gridspec_kw={'width_ratios': (3,1), 'height_ratios': (3,1)})

for b in set(upome.values):
    sel = pca.loc[upome.values == b].values
    sel_divs = divs.loc[upome.values == b]
    axes[0,0].scatter(-sel.T[0], sel.T[1], s=1, label=b, c=biome2color[b])

    axes[1,0].scatter(-sel.T[0], sel_divs['gf_1m_rich'], s=1,
            alpha=.1,
            c='k')

    axes[0,1].scatter(
        sel_divs['cog525_1m_rich'],
           sel.T[1], alpha=.1,
           s=1, c='k')

    axes[1,0].scatter(-sel.T[0], sel_divs['gf_1m_rich'], s=1,
            alpha=.1,
            c=biome2color[b])

    axes[0,1].scatter(
        sel_divs['cog525_1m_rich'],
           sel.T[1], alpha=.1,
           s=1, c=biome2color[b])

lm = linear_model.LinearRegression()
lm.fit(-pca.values.T[:1].T, divs[['gf_1m_rich']].values)
pmax = -pca.values.T[0].min()
pmin = -pca.values.T[0].max()
axes[1,0].plot([pmin*1.2, pmax*1.2],
    lm.predict([[pmin*1.2],[pmax*1.2]]),
    lw=2, c='red')

lm.fit(divs[['cog525_1m_rich']].values, pca.values.T[1].T)
pmin = divs['cog525_1m_rich'].min()
pmax = divs['cog525_1m_rich'].max()

axes[0,1].plot(
        [pmin, pmax*1.05],
    lm.predict([[pmin],[pmax*1.05]]),
    lw=2, c='red')

axes[0,0].legend(loc='best')
sns.despine(fig, trim=True)
fig.savefig('plots/SupplFigure-PCoA.svg')
