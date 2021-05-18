from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
abundance = pd.read_table('tables/rare-abundance.tsv', index_col=0)
abundance = abundance[~biome.map({'amplicon', 'isolate'}.__contains__)]
abundance = abundance[abundance['total_sum'] > 1e6]


X = pd.melt(pd.DataFrame(
        {
        'Abundance' : abundance.eval('rare_sum/total_sum'),
        'Count' : abundance.eval('rare_count/total_count'),
        'Habitat' : biome.reindex(abundance.index)
        }), id_vars=['Habitat'])
X['value'] *= 100

fig,ax = plt.subplots()
order = abundance.eval('rare_sum/total_sum').groupby(biome).median().sort_values().index

sns.boxplot(y='Habitat', x='value', hue='variable', ax=ax, data=X, order=order, width=.5, fliersize=.25)
#for x in ax.get_xticklabels():
#    x.set_rotation(90)
ax.set_xlabel("Percentage")
fig.tight_layout()
fig.savefig('plots/SuppFigure-rare-fractions.pdf')
fig.savefig('plots/SuppFigure-rare-fractions.svg')
