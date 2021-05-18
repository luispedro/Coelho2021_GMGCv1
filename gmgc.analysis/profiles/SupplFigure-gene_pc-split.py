import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt

import seaborn as sns
import pandas as pd


from scipy import stats
import matplotlib
matplotlib.use('Agg')

biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
total = computed.insertsHQ

print("Loaded basic")

meta = pd.read_table('/g/bork1/coelho/DD_DeCaF/genecats.cold/selected-cleaned-metadata-100.tsv', index_col=0)
marine_index = biome.index[biome == 'marine']
meta_marine = meta.loc[marine_index]


csamples = pd.read_table('tables/cogs.counts.txt', index_col=0)
scotu = csamples.iloc[:,1:].mean(1)
gene = csamples['0']
gene_pc = gene/scotu
groups = [] 
    
fig,ax = plt.subplots()
for f  in set(meta_marine.env_feature) : 
    sel = meta_marine.index[meta_marine.env_feature == f]
    if len(sel) < 20:
        continue
    values = gene_pc[sel].values
    values.sort()
    sns.distplot(values[1:-1], label=f, hist=False, ax=ax)
    groups.append(values)
    
    
ax.legend(loc='best')
ax.set_xlabel('Number of conspecific genes')
ax.set_ylabel('Density')

fig.tight_layout()
fig.savefig('plots/gpscotu-marine-sub.svg')
print(stats.kruskal(*groups))

meta_soil = meta.loc[meta.study_accession == 'ERP009498']

groups = []
fig,ax = plt.subplots()

for f in set(meta_soil['soil_taxonomic/local classification']):
    sel = meta_soil['soil_taxonomic/local classification'] == f
    sel = sel.index[sel]
    
    if len(sel) < 20: continue
    values = gene_pc[sel].values
    values.sort()
    sns.distplot(values[1:-1], label=f, hist=False, ax=ax)
    groups.append(values)

    
ax.legend(loc='best')
ax.set_xlabel('Number of conspecific genes')
ax.set_ylabel('Density')

fig.tight_layout()
fig.savefig('plots/gpscotu-soil-sub.svg')
print(stats.kruskal(*groups))
