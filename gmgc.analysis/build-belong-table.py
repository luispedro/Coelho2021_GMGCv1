from glob import glob
import numpy as np
import pandas as pd

    
prevalence = pd.read_table('tables/genes.unique.prevalence.txt', index_col=0)
prevalence.rename(columns=lambda c : c.replace(' ', '-'), inplace=True)
prev5 = prevalence >= 5

subcatalogs = {}
for f in glob('cold/subcatalogs/*.fna'):
    if 'complete' in f:
        continue
    biome = f.split('/')[-1].split('.')[1]
    cur = []
    for line in open(f):
        if line[0] == '>':
            cur.append(line[1:-1])
    cur.sort()
    subcatalogs[biome] = cur
    
    
allgenes = [line.strip() for line in open('cold/derived/GMGC10.headers')]

belong = pd.DataFrame(np.zeros((len(allgenes), len(prev5.columns)), dtype=int), index=allgenes, columns=prev5.columns)

for b in subcatalogs:
    belong[b][subcatalogs[b]] = 1

for b in prev5.columns:
    s = prev5[b]
    s = s.index[s]
    belong[b][s] *= 2
    
belong.to_csv('cold/GMGC10.gene-environment.tsv', sep='\t')
