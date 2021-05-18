import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot as plt
import pandas as pd
from matplotlib import style
from scipy import stats

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
mapped = mapped[mapped.biome == 'human gut']
pos = pd.read_table('cold/gps-coords.tsv', index_col=0)
pos = pos.reindex(mapped.index)


print(stats.mannwhitneyu(mapped.loc[pos[pos.latitude >= 0].index].fr12, mapped.loc[pos[pos.latitude < 0].index].fr12, alternative='two-sided'))


mapped['hemisphere'] = (pos.latitude >= 0).map(lambda ix: ['South', 'North'][ix])
mapped = mapped.melt(id_vars=['biome', 'hemisphere'], var_name='reference')
mapped['reference'] =  mapped.reference.map({'fr12': 'genomes', 'gmgc' : 'GMGC'})

fig,ax = plt.subplots()
#sns.swarmplot(hue='hemisphere', y='value', x='reference', dodge=True, data=mapped, ax=ax, size=1)
sns.boxplot(hue='hemisphere', y='value', x='reference', dodge=True, data=mapped, ax=ax)
fig.tight_layout()

sns.despine(fig, trim=True)
fig.savefig('plots/Hemisphere.png', dpi=300)
fig.savefig('plots/Hemisphere.svg')
