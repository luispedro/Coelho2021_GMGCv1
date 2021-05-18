import numpy as np
import pandas as pd

def build_rflow(g):
    habitats = list(g.columns)
    rflow = pd.DataFrame(0, index=habitats, columns=habitats)
    for i in range(len(habitats)):
        for j in range(i+1, len(habitats)):
            h0  = habitats[i]
            h1 = habitats[j]
            rflow.loc[h0,h1] = np.sum(g[h0].values & g[h1].values)
            rflow.loc[h1,h0] = rflow.loc[h0,h1]
    print('build rflow')
    uniq = g.sum(1) ==1

    rflow['unshared'] = 0
    rflow['total'] = 0
    for h in habitats:
        rflow.loc[h, 'unshared'] = np.sum(uniq.values & g[h].values)
        rflow.loc[h, 'total'] = np.sum(g[h].values)
    return rflow

genv = pd.read_feather('cold/GMGC10.gene-environment.feather' , nthreads=24)
genv = genv.drop(['index', 'all', 'isolate', 'amplicon'], axis=1)
print('loaded env')
rflow = build_rflow(genv)
rflow.to_csv('tables/raw-flow.2.tsv', sep='\t')
print('done')



up = {
   'dog gut': 'mammal gut',
   'cat gut': 'mammal gut',
   'human blood plasma': 'other human',
   'human gut': 'mammal gut',
   'human nose': 'other human',
   'human oral': 'other human',
   'human skin': 'other human',
   'human vagina': 'other human',
   'mouse gut': 'mammal gut',
   'pig gut': 'mammal gut',
}

gup = genv.groupby(lambda c: up.get(c.replace('-' , ' '),c), axis=1).sum()
rflow  = build_rflow(gup > 0)

rflow.to_csv('tables/raw-flow-up.2.tsv', sep='\t')
print('done')

