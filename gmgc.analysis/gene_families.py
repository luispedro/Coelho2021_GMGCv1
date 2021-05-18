from jug import TaskGenerator

@TaskGenerator
def count_gene_names():
    import pandas as pd
    from collections import Counter
    gene_names = Counter()
    for chunk in pd.read_table('cold/annotations/GMGC.95nr.emapper.annotations', chunksize=1000000, header=None, usecols=[4]):
        gene_names.update(chunk[4])
    return dict(gene_names)

