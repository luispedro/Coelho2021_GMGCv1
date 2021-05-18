import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
from matplotlib import style

style.use('seaborn-notebook')
from seaborn import apionly as sns

biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)

mapstats_fr12 = pd.read_table('cold/profiles/freeze12only.mapstats.txt', index_col=0, comment='#').T
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
input_fr12 = pd.read_table('../../profiles-all/freeze12only.input-stats.parsed.txt', index_col=0)
mapstats_fr12['total'] = input_fr12['inserts']

fractions_fr12 = mapstats_fr12['aligned']/mapstats_fr12['total']


total = pd.read_table('tables/total.tsv', index_col=0)
fractions_gmgc = total['total']/computed.insertsHQ

mapped = pd.DataFrame({'fr12' : fractions_fr12, 'gmgc': fractions_gmgc, 'biome' : biome})
mapped = mapped[mapped.biome != 'isolate']
bs = list(set(mapped.biome))
maprates = mapped.groupby('biome').mean()
bs.sort(key=lambda b: maprates.loc[b].values[0])

for sel in ('fr12', 'gmgc'):
    fig,ax = plt.subplots()
    sns.swarmplot(x='biome', y=sel, data=mapped, order=bs, color='#000000', ax=ax, size=1)
    ax.set_ylabel("Fraction of short reads mapped")
    for x in ax.get_xticklabels():
        x.set_rotation(90)

    sns.despine(fig, offset=4, trim=True)
    fig.tight_layout()
    fig.savefig(f'plots/mapping-rates-{sel}.svg')
    fig.savefig(f'plots/mapping-rates-{sel}.png', dpi=150)


fig,ax = plt.subplots()

maprates = maprates.sort_values('gmgc')
maprates = maprates[::-1]

ax.plot(maprates.fr12.values, label='Reference')
ax.plot(maprates.gmgc.values, label='GMGC')
ax.legend(loc='best')
ax.set_ylabel("Fraction of short-reads recruited")
ax.set_xlabel("Habitat")
ax.set_xticks(np.arange(len(maprates)))
ax.set_xticklabels(maprates.index)
for x in ax.get_xticklabels():
    x.set_rotation(90)

sns.despine(fig, offset=True, trim=True)
fig.tight_layout()
fig.savefig("plots/mapping-rates.pdf")
fig.savefig("plots/mapping-rates.svg")
fig.savefig("plots/mapping-rates.png")
