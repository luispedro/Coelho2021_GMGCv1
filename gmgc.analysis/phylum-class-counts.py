import pandas as pd
from collections import Counter
from taxonomic import ncbi

taxon_count = Counter()
for chunk in pd.read_table('cold/GMGC10.taxonomic.reconciliation.map', chunksize=100_000, usecols=[3], squeeze=True):
    taxon_count.update(chunk.values)

n = ncbi.NCBI()


phclass = Counter()
for k,v in taxon_count.items():
    kl = n.at_rank(str(k), 'class')
    p = n.at_rank(str(k), 'phylum')
    phclass[p,kl] += v
    

data = pd.DataFrame([(ph,cl,c) for (ph,cl),c in phclass.items()], columns=['phylum', 'class', 'count'])
data.sort_index(inplace=True)
data.to_csv('tables/phylum-class.counts.txt', sep='\t', index=False)
