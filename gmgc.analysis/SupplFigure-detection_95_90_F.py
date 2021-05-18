from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

def expand(h):
    expanded = np.zeros(h['detections'].max()+1, int)
    for _,r in h.iterrows():
        expanded[r['detections']] = r['count']
    return expanded

hists95 = pd.read_table('tables/genes.prevalence.hists.txt', index_col=0)
hists95 = hists95['all']
hists95 = expand(
                hists95
                    .reset_index()
                    .rename(columns={'index':'detections', 'all':'count'}))

hists90 = pd.read_table('tables/GMGC.90nr.gene.hists.txt', index_col=0)
hists90 = hists90['all']
hists90 = expand(
                hists90
                    .reset_index()
                    .rename(columns={'index':'detections', 'all':'count'}))

histsF = pd.read_table('tables/detections_per_biome.txt', )
histsF = histsF.query('origin == "all"')
histsF = expand(histsF)
histsF = np.concatenate([[0],-np.diff(histsF[1:])])

fig,ax = plt.subplots()

ax.plot(100 * np.cumsum(hists95)/hists95.sum(), '-', label='95% non-redundant')
ax.plot(100 * np.cumsum(hists90)/hists90.sum(), '-', label='90% non-redundant')
ax.plot(100 * np.cumsum(histsF)/histsF.sum(), '-', label='gene families')
ax.set_xlim(0,100)
ax.set_yticks([0,25,50,75,100])
ax.set_ylabel("Fraction of genes (%)")
ax.set_xlabel("Number of detections (cumulative)")
ax.legend(loc='best')

ax.grid(True, axis='y')
sns.despine(fig, trim=True)
fig.tight_layout()

fig.savefig('plots/SupplFigure-detection_fraction.svg')
