import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
from matplotlib import style

style.use('default')
coords = pd.read_table('cold/gps-coords-country.tsv', index_col=0)
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)

mapstats_fr12 = pd.read_table('cold/profiles/freeze12only.mapstats.txt', index_col=0, comment='#').T
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
input_fr12 = pd.read_table('../profiles-all/freeze12only.input-stats.parsed.txt', index_col=0)
mapstats_fr12['total'] = input_fr12['inserts']

fractions_fr12 = mapstats_fr12['aligned']/mapstats_fr12['total']


total = pd.read_table('tables/total.tsv', index_col=0)
fractions_gmgc = total['total']/computed.insertsHQ

mapped = pd.DataFrame({'fr12' : fractions_fr12, 'gmgc': fractions_gmgc, 'biome' : biome})
mapped = mapped[mapped.biome == 'human gut']

mapped['country'] = coords.reindex(mapped.index).country
mapped['country'] = mapped.country.map(lambda c :{
        'United Kingdom of Great Britain and Northern Ireland': 'UK',
        'Russian Federation': 'Russia',
        'United States of America': 'USA'}.get(c,c))

order = mapped.groupby('country').median().fr12.sort_values().index
mapped_country = pd.melt(mapped.drop(['biome'],axis=1), id_vars=['country']).rename(columns={'variable':'reference', 'value': 'mapped'})
mapped_country['reference'] = mapped_country.reference.map({'fr12':'genomes', 'gmgc': 'GMGC'})

fig,ax = plt.subplots()
sns.boxplot(x='country', y='mapped', hue='reference', dodge=True, order=order[::-1], data=mapped_country, ax=ax)
fig.tight_layout()
fig.savefig('plots/MappingPerCountry.svg')
fig.savefig('plots/MappingPerCountry.pdf')




mapped_ig = pd.melt(mapped[['fr12', 'gmgc', 'income_group']], id_vars=['income_group']).rename(columns={'variable': 'reference', 'value':'mapped'})
mapped_ig['reference'] = mapped_ig.reference.map({'fr12':'genomes', 'gmgc': 'GMGC'})

order = ['High income',  'Upper middle income', 'Lower middle income']

fig,ax = plt.subplots()
sns.violinplot(x='income_group', y='mapped', hue='reference', dodge=True, order=order, data=mapped, ax=ax)
fig.tight_layout()
fig.savefig('plots/MappingPerIncomeGroup.pdf')
fig.savefig('plots/MappingPerIncomeGroup.svg')

