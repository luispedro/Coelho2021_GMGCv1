import pandas as pd
meta  = pd.read_table('cold/GMBC10.meta.tsv', index_col=0, )
bac120 = pd.read_table('cold/GMBC/gtdbtk.bac120.summary.tsv', index_col=0)
ar122 = pd.read_table('cold/GMBC/gtdbtk.ar122.summary.tsv', index_col=0)
classification_bac120 = bac120['classification'].rename(index=lambda ix: ix[:-len('.fna')])
classification_ar122 = ar122['classification'].rename(index=lambda ix: ix[:-len('.fna')])
meta['GTDB_tk'] = pd.concat([classification_bac120,classification_ar122]).reindex(meta.index).fillna('-')

meta.to_csv('cold/GMBC10.meta.w_gtdbtk.tsv',sep='\t')
