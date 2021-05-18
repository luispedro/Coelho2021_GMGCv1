import scipy.cluster.hierarchy
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import style
style.use('seaborn-notebook')
import seaborn as sns
import matplotlib.gridspec as gridspec

# %matplotlib qt


names = {
    'J': 'Translation, ribosomal structure and biogenesis',
    'A': 'RNA processing and modification',
    'K': 'Transcription',
    'L': 'Replication, recombination and repair',
    'B': 'Chromatin structure and dynamics',
    'D': 'Cell cycle control, cell division, chromosome partitioning',
    'Y': 'Nuclear structure',
    'V': 'Defense mechanisms',
    'T': 'Signal transduction mechanisms',
    'M': 'Cell wall/membrane/envelope biogenesis',
    'N': 'Cell motility',
    'Z': 'Cytoskeleton',
    'W': 'Extracellular structures',
    'U': 'Intracellular trafficking, secretion, and vesicular transport',
    'O': 'Posttranslational modification, protein turnover, chaperones',
    'C': 'Energy production and conversion',
    'G': 'Carbohydrate transport and metabolism',
    'E': 'Amino acid transport and metabolism',
    'F': 'Nucleotide transport and metabolism',
    'H': 'Coenzyme transport and metabolism',
    'I': 'Lipid transport and metabolism',
    'P': 'Inorganic ion transport and metabolism',
    'Q': 'Secondary metabolites biosynthesis, transport and catabolism',
    'R': 'General function prediction only',
    'S': 'Function unknown',
}


biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
f = pd.read_feather('tables/highNOGS.feather')
f.set_index('index', inplace=True)
f = (f / f.sum()).T

pb = f.groupby(biome).mean()
pb.drop('isolate', inplace=True)
pb.drop('amplicon', inplace=True)
pb = pb.drop('-uncharacterized-', axis=1)
pb = pb.drop('S', axis=1)
pb = pb.T[pb.max() > 0.01].T.rename(columns=names)
pb = pb[pb.sum().sort_values().index[::-1]]
pb = (pb.T/ pb.sum(1)).T

fig = plt.figure()

gs = gridspec.GridSpec(2, 1, height_ratios=[1,3])


axes = [
    plt.subplot(gs[1]),
    plt.subplot(gs[0])]

Z = scipy.cluster.hierarchy.average(pb.values)
dendogram = scipy.cluster.hierarchy.dendrogram(Z, orientation='top', ax=axes[1])
pbplot = pb.T[pb.max() > 0.01].T.rename(columns=names)
pbplot.iloc[dendogram['leaves']].plot(kind='bar', stacked=True, ax=axes[0])
axes[1].set_xticks([])
axes[1].set_yticks([])
sns.despine(fig)
fig.tight_layout()
fig.savefig("plots/hiNOGS.svg")
fig.savefig("plots/hiNOGS.pdf")
fig.savefig("plots/hiNOGS.png", dpi=300)
