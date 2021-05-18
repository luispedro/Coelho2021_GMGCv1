# %matplotlib qt

import pandas as pd
import numpy as np
import seaborn as sns
from scipy import stats
from matplotlib import pyplot as plt
from matplotlib import style
style.use('default')

families = pd.read_feather('gmgc_selection/families.feather')

fam_size = families.fam_size
# Because we plot means/distributions without outliers, it does not matter
# whether we remove values > 2.0 as they are only a small fraction (~0.01% of
# the total)
#families = families.query('dnds <= 2.0')
families['abundance_cat'] = pd.qcut(families['abundance'], 5, labels=['Q{}'.format(i+1) for i in range(5)])
families = families.dropna()

fam_size = fam_size.sort_values(ascending=False)
fam_fraction = fam_size.cumsum()/fam_size.sum()
cutoffs = [
    np.where(fam_fraction.values > ft)[0][0]
    for ft in [.2, .4, .6, .8]]
cutoffs = [ fam_size.iloc[c] for c in cutoffs[::-1]]

def cut_off(s):
    if s < cutoffs[0]: return 'G1'
    if s < cutoffs[1]: return 'G2'
    if s < cutoffs[2]: return 'G3'
    if s < cutoffs[3]: return 'G4'
    return 'G5'
cutoff = np.where(fam_fraction.values >.5)[0][0]
cutoff = fam_size.iloc[cutoff]
families['is_large'] = families.eval('fam_size > @cutoff')
families['size_cat_w'] = families.fam_size.map(cut_off)

fig, axes = plt.subplots(1,2, sharey=True)

sns.boxplot(x='abundance_cat',
        y='dnds',
        data=families,
        ax=axes[0],
        showfliers=False,
        width=.5,
        boxprops={'fill':0, 'color':'black'},
        saturation=1,
        )
axes[0].set_xlabel('Abundance quintile')

sns.boxplot(x='size_cat_w',
        y='dnds',
        data=families,
        ax=axes[1],
        showfliers=False,
        width=.5,
        boxprops={'fill':0, 'color':'black'},
        saturation=1,
        )
axes[1].set_xlabel("Gene family size category")
sns.despine(fig, trim=True)

print('''
        Kruskal (panel a): {0[1]}
        Kruskal (panel b): {1[1]}
        '''.format(
            stats.kruskal(*[families.loc[ixs].dnds for ixs in
                            families.groupby('abundance_cat').groups.values()]),
            stats.kruskal(*[families.loc[ixs].dnds for ixs in
                            families.groupby('size_cat_w').groups.values()]),))
fig.savefig('plots/SupplFigure-dnds_per_family.svg')
