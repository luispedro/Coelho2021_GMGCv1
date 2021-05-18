from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd

fraction = pd.read_table('tables/MAGgenes.detections.tsv', index_col=0)
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)

fraction = fraction[~biome.map({'amplicon', 'isolate'}.__contains__)]
fraction['Habitat'] = biome.reindex(index=fraction.index)
fraction['Missing'] = 100. - 100.*fraction['unique_only']
order = fraction['Missing'].groupby(biome).median().sort_values().index

fig,ax = plt.subplots()
sns.swarmplot(x='Habitat', y='Missing', order=order, color='k', size=1., data=fraction, ax=ax)

for x in ax.get_xticklabels():
    x.set_rotation(90)

ax.set_ylabel('Missing genes in MAGs (%)')
fig.tight_layout()
fig.savefig('SupplFigure-fractionMAG.svg')
fig.savefig('SupplFigure-fractionMAG.png', dpi=150)

