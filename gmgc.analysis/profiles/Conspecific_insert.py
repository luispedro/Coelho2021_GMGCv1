from scipy import stats
import seaborn as sns
import pandas as pd
from matplotlib import pyplot as plt

biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
csamples = pd.read_table('tables/cogs.counts.txt', index_col=0)
n_families = pd.read_table('tables/number_clusters_per_sample_mayMen10.txt', index_col=0, squeeze=True)


computed = pd.read_table('cold/sample.computed.tsv', index_col=0)

def remove_outliers(xs):
    
    xs.sort_values()
    n = len(xs)//20
    return xs[n:-n]

min_x=50


X, Y = csamples.iloc[:,1:].mean(1), csamples['0']
min_n = 100

data = []
for b in set(biome.values):
    if b in {'amplicon', 'isolate'}: continue
    x = X[biome == b]
    x = x[x >= min_x]
    if len(x) < min_n:
       continue
    y = Y[x.index]
    vs = remove_outliers(y/x)
    data.append(vs)
data = pd.concat(data)
data = pd.DataFrame({'r' : data, 'biome': biome.reindex(data.index)})


fig,ax = plt.subplots()
ax.scatter(data.r,  computed['insertsHQ'].reindex(data.index)/1e6, s=1,  alpha=.1, c='k')
sns.despine(fig, trim=True)
ax.set_xlabel('Number of genes per species')
ax.set_ylabel('Number of inserts (millions)')
fig.savefig('plots/Conspecific_insert.png', dpi=300)

fig,axes = plt.subplots(2,1, sharex=True)
for b,ax in zip(['marine', 'soil'], axes.flat):
    sel = data.query('biome == @b')
    ax.scatter(sel.r,  computed['insertsHQ'].reindex(sel.index)/1e6, alpha=1, s=2,  c='k')
    ax.set_title(b)
    ax.set_xlabel('Genes per species')
    ax.set_ylabel('Nr reads (millions)')
    
sns.despine(fig, trim=True)
fig.tight_layout()
fig.savefig('plots/Conspecific_insert_soil+marine.png', dpi=300)
