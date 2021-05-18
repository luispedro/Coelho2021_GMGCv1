import pandas as pd
from collections import Counter

ngenes = Counter()
for chunk in pd.read_table('orf-stats.tsv', usecols=[0], chunksize=1000000):
    ngenes.update(chunk['gene'].map(lambda g: g.split('_')[0]).values)
ng = pd.Series(dict(ngenes))
computed = pd.read_table('sample.computed.tsv', index_col=0)

computed['nr_orfs'] = ng.reindex(index=computed.index).fillna(0).astype(int)
computed.to_csv('sample.computed.new.tsv', sep='\t')
