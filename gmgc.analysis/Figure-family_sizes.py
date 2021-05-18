%matplotlib qt
from matplotlib.lines import Line2D
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

ONLY_COMPLETE = False

import seaborn as sns
from matplotlib import style
style.use('default')

if ONLY_COMPLETE:
    families = pd.read_table('tables/fragments/gmgc_filter_families/complete_genes_per_family.tsv', index_col=0,
        names=['gene_family', 'fam_size',  'num_complete_genes',  'num_incomplete_gene'])
    fam_size = families.query('num_complete_genes > 0')['num_complete_genes']
    fam_size = fam_size.reset_index()['num_complete_genes'].copy()
else:
    families = pd.read_feather('gmgc_selection/families.feather')
    fam_size = families.fam_size.copy()
    fam_size = fam_size.reset_index()['fam_size'].copy()

del families
fam_size.sort_values(inplace=True, ascending=False)
fam_size_cum = fam_size.cumsum()/fam_size.sum()


fig, ax = plt.subplots()
Y_FAMILIES = .2
Y_GENES = 0
ax.set_ylim(-.1,1.3)
ax.set_xlim(-.1,1.1)


ax.add_line(Line2D([0,1], [Y_FAMILIES, Y_FAMILIES], lw=1, c='k'))
ax.add_line(Line2D([0,1], [Y_GENES, Y_GENES], lw=1, c='k'))
ax.add_line(Line2D([0,0], [Y_GENES, Y_FAMILIES], c='k'))
ax.add_line(Line2D([1,1], [Y_GENES, Y_FAMILIES], c='k'))

cut50raw = np.where(fam_size_cum >= .5)[0][0]
cut50 = cut50raw/len(fam_size_cum)
cut_singletons_raw = np.where(fam_size == 1)[0][0]
cut_singletons = cut_singletons_raw/len(fam_size)
cut_singletons_genes = 1-(cut_singletons_raw)/(101e6 if ONLY_COMPLETE else 302e6)

cut_doubletons_raw = np.where(fam_size == 2)[0][0]
cut_doubletons = cut_doubletons_raw/len(fam_size)
cut_doubletons_genes = 1-(cut_singletons_raw + cut_doubletons_raw*2)/(101e6 if ONLY_COMPLETE else 302e6)

ax.add_line(Line2D([.5,cut50], [Y_GENES, Y_FAMILIES], c='k'))
ax.add_line(Line2D([cut_singletons_genes,cut_singletons], [Y_GENES, Y_FAMILIES], c='k'))
ax.add_line(Line2D([cut_doubletons_genes,cut_doubletons], [Y_GENES, Y_FAMILIES], c='k'))
ax.plot(np.arange(len(fam_size), dtype=float)/len(fam_size), np.log10(fam_size)/10 + .02 + Y_FAMILIES, c='k')

for p in range(5):
    p = p/10 + .02 + Y_FAMILIES
    ax.add_line(Line2D([0,1], [p, p], c='k'))

fig.savefig('plots/Figure_sizes_nr_complete.svg'
        if ONLY_COMPLETE else 'plots/Figure_sizes.svg')

print('''
        Largest gene family: {}
        Index of gene family at cutoff 50: {}
        Size of gene family at cutoff 50: {}
        Percentage singletons: (families: {:.1%}; genes: {:.1%})
        Percentage doubletons: (families: {:.1%}; genes: {:.1%})
'''.format(
    fam_size.iloc[0],
    cut50raw,
    fam_size[cut50raw],
    1-cut_singletons, 1-cut_singletons_genes,
    1-cut_doubletons, 1-cut_doubletons_genes,
))


