import numpy as np
from collections import defaultdict
import pandas as pd

genes = pd.read_table('cold/freeze12.rename.table', index_col=0, usecols=[0,1], header=None)

genes['genome'] = genes[1].map(lambda g: '.'.join(g.split('.')[:2]))

genome2genes = genes.groupby('genome').groups

genome2genes_catalog = defaultdict(set)
fr12_rep = pd.read_table('cold/GMGC.relationships.txt.fr12.catalog.sorted', header=None, usecols=[0,2], names=['orig', 'rep'])


fr12_2_rep = fr12_rep.groupby('orig').groups

catalog_genes = [line.strip() for line in open('cold/derived/GMGC.95nr.old-headers.sorted')]
catalog = set(catalog_genes)
redundant = set(line.strip() for line in open('cold/redundant.complete.sorted.txt'))
catalog -= redundant

print(5)

fr12_rep_d = fr12_rep.rep.to_dict()

hists = pd.read_table('./tables/genes.prevalence.hists.txt', index_col=0)                                     
print(6)

prevalence = pd.read_feather('tables/genes.unique.prevalence.feather')
prevalence.set_index('index', inplace=True)
print(7)

rare = prevalence['all'] <= 10
rare_ixs = rare.index[rare]
rare_names = set(catalog_genes[i] for i in rare_ixs)
print(8)


fraction_rare = {}
for k,gs in genome2genes.items():
    gsc = set()
    for g in gs:
        if g in catalog:                       
            gsc.add(g)
        else:
            try:
                gsc.update([fr12_rep_d[r] for r in fr12_2_rep[g]])
            except:
                pass
    f = len(gsc & rare_names)/len(gsc)
    fraction_rare[k] = [len(gsc), f]

fraction_rare = pd.DataFrame(fraction_rare, index=['nr_genes', 'fraction_rare']).T
fraction_rare.to_csv('tables/fraction-rare.tsv', sep='\t')
