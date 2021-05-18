import pandas as pd
c90 = pd.read_table('cold/GMGC.95nr.90lc_cluster.tsv', header=None, index_col=1, squeeze=True)
gf = pd.read_table('cold/GMGC.gf.cluster_members.tsv', header=None, index_col=0, squeeze=True)
clusters=  pd.DataFrame({'cluster90': c90, 'gene.family': gf})
clusters302 = clusters.dropna()
clusters302.to_csv('cold/GMGC.clusters.tsv', sep='\t', index_label='unigene')
