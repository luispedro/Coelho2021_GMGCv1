import pandas as pd
import numpy as np
from scipy import stats

hists1m = pd.read_table('tables/genes.prevalence.1m.hists.txt', index_col=0)
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
tables = {}
data = []
for N in [10, 100]:
    for b in hists1m.columns:
        gp = hists1m[b].values[1:]
        if b == 'all':
            n = 12743
        else:
            n = (biome == b).sum()
        data.append((b, N, n, *stats.pearsonr((1+gp[:N]), 1./(np.arange(1,N+1)))))
data = pd.DataFrame(data, columns=['Habitat', 'Max_k', 'N', 'R', 'P_value'])

