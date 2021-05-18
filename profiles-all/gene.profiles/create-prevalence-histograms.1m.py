import pandas as pd
import numpy as np
prevalence = pd.read_feather('tables/genes.1m.unique.prevalence.feather')
prevalence.set_index('index', inplace=True)


hists = {}
for c in prevalence.columns:
    if c in ['amplicon', 'isolate']:
        continue
    hists[c] = np.bincount(prevalence[c])

size = max(len(v) for v in hists.values())
nhists = {}
for k,v in hists.items():
    c = np.zeros(size, dtype=int)
    c[:len(v)] = v
    nhists[k] = c

nhists = pd.DataFrame(nhists)
nhists.to_csv('tables/genes.prevalence.1m.hists.txt', sep='\t')
