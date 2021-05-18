
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
from sklearn import linear_model
import brewer2mpl

import seaborn as sns
import pandas as pd

biome = pd.read_table('../../gmgc.analysis/cold/biome.txt', squeeze=True, index_col=0)
cmap = brewer2mpl.get_map('Paired', 'Qualitative', 12)

computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
valid = computed.insertsHQ > 1e6
c = computed[valid]

fig,ax = plt.subplots()

i = 0
for b in set(biome.values):
  cb = c[c.biome == b]
  x = cb.insertsHQ / 1_000_000
  y = cb.nr_orfs
  if b in {'isolate', 'amplicon'}: continue
  ax.scatter(x, y , s=0.1, c='k') # cmap.mpl_colors[i])
  i += 1

# ax.legend(loc='best')
ax.set_yscale('log')
ax.set_xscale('log')
fig.savefig('plots/nr-orfs-per-insert-log.svg')
fig.savefig('plots/nr-orfs-per-insert-log.png', dpi=300)
