import pickle
import pandas as pd
rename = pickle.load(open('cold/rename.pkl', 'rb'))
meta = pd.read_table('cold/selected-cleaned-metadata-100.tsv', index_col=0)
negatives = meta.query('sample_source == "negative control"').index
negative_genes = set()

for f in negatives:
    f = pd.read_table('outputs.txt/{}.unique.txt.gz'.format(f), index_col=0)
    negative_genes.update(f.index)
negative_genes = list(negative_genes)
with open('cold/contaminants.txt', 'wt') as output:
    for g in negative_genes:
        n = rename[g]
        output.write(f'{g}\t{n}\n')
