import seaborn as sns
import pandas as pd
data =  pd.read_table('gene-counts-per-sample.txt', sep=' ', index_col=0, squeeze=True, header=None)
unique = data.loc[data.index.map(lambda s: 'unique' in s)].rename(index=lambda s : s[:-len('.unique.txt.gz')])
nunique = data.loc[data.index.map(lambda s: 'unique' not in s)].rename(index=lambda s : s[:-len('.txt.gz')])
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
from matplotlib import pyplot as plt

computed['nunique'] = nunique
computed['unique'] = unique


computed = computed[computed['insertsHQ'] >= 1e6]

computed['insertsHQ'] /= 1e6


fig,ax = plt.subplots()
ax.scatter(computed['insertsHQ'], computed['unique'], s=.1, c='k')
ax.set_xlabel('Number of inserts (milions)')
ax.set_ylabel('Number of detected genes (unique)')
ax.set_xscale('log')
ax.set_yscale('log')
sns.despine(fig, offset=True, trim=4)
fig.tight_layout()
fig.savefig('plots/unique-per-insert.svg')
fig.savefig('plots/unique-per-insert.png')

fig,ax = plt.subplots()
ax.scatter(computed['insertsHQ'], computed['nunique'], s=.1, c='k')
ax.set_xlabel('Number of inserts (milions)')
ax.set_ylabel('Number of detected genes')
ax.set_xscale('log')
ax.set_yscale('log')
sns.despine(fig, offset=True, trim=4)
fig.tight_layout()
fig.savefig('plots/nunique-per-insert.svg')
fig.savefig('plots/nunique-per-insert.png')
