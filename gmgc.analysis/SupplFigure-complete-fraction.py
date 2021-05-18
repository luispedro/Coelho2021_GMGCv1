# %matplotlib qt

import pandas as pd
from matplotlib import pyplot as plt

import seaborn as sns
import pandas as pd
from matplotlib import style
style.use('default')

mapped_complete = pd.read_table('tables/complete-gene-fraction-mapped.tsv', index_col=0)
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
fraction_complete = mapped_complete.eval('active_sum/total_sum')

total = pd.read_table('tables/total.tsv', index_col=0)
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
fractions_gmgc = total['total']/computed.insertsHQ

data = pd.DataFrame({
    'All ORFs':fractions_gmgc,
    'Complete only':fraction_complete,
    'habitat':biome,
    }).melt(id_vars=['habitat'], value_vars=['All ORFs', 'Complete only'])

data.rename(columns={'variable':'subset', 'value':'fraction_mapped'}, inplace=True)
data = data[~data.habitat.map({'isolate', 'amplicon'}.__contains__)]

fig,ax = plt.subplots()
ax.clear()

sns.boxplot(
        x='habitat',
        hue='subset',
        y='fraction_mapped',
        hue_order=['Complete only', 'All ORFs'],
        width=.5,
        fliersize=1,
        linewidth=1,
        data=data,
        ax=ax)
for x in ax.get_xticklabels():
    x.set_rotation(90)

sns.despine(fig, trim=True)
fig.tight_layout()
fig.savefig('plots/only-complete-vs-all-orfs.svg')
