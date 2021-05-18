from glob import glob
from jug import TaskGenerator, CachedFunction, barrier
from projections import load_gf_projection

def blocks_of(xs, n):
    xs = list(xs)
    while xs:
        yield xs[:n]
        xs = xs[n:]

_gfd = None

@TaskGenerator
def project_files(us):
    import pandas as pd
    global _gfd
    if _gfd is None:
        _gfd = load_gf_projection()
    for u in us:
        s = u.split('/')[1].split('.')[0]
        f = pd.read_table(u, index_col=0, squeeze=True)
        gf = f.groupby(_gfd).sum()
        ofile = u.replace('outputs.txt', 'gf.txt')
        pd.DataFrame({s:gf}).to_csv(ofile, sep='\t')

@TaskGenerator
def maximum_of(us):
    import pandas as pd
    data = []
    for u in us:
        u = u.replace('outputs.txt', 'gf.txt')
        try:
            f = pd.read_table(u, index_col=0, squeeze=True)
        except:
            u = u.replace('.gz','')
            f = pd.read_table(u, index_col=0, squeeze=True)
        f /= f.sum()
        data.append(f)
    data = pd.DataFrame(data)
    data.fillna(0, inplace=True)
    mdata = data.max()
    return mdata

@TaskGenerator
def global_max(mv):
    import pandas as pd
    mv = pd.DataFrame(mv)
    mv.fillna(0, inplace=True)
    final = mv.max()
    final.sort_values(inplace=True)
    return final

@TaskGenerator
def extract_active(final, minv):
    return final.index[final >= minv]

@TaskGenerator
def load_submatrix(us, active):
    import pandas as pd
    data = {}
    for u in us:
        u = u.replace('outputs.txt', 'gf.txt')
        s = u.split('/')[1].split('.')[0]
        try:
            f = pd.read_table(u, index_col=0, squeeze=True)
        except:
            u = u.replace('.gz','')
            f = pd.read_table(u, index_col=0, squeeze=True)
        data[s] = f.reindex(active, fill_value=0)
    return pd.DataFrame(data)

@TaskGenerator
def save_final_table(table):
    import pandas as pd
    table = pd.concat(table, axis=1)
    table.to_csv('tables/gf.scaled.tsv', sep='\t')
    n = table.index.map(lambda ix:int(ix.split('.')[1], 10))
    table.reset_index(inplace=True)
    table['index'] = n
    table.to_feather('tables/gf.scaled.feather')
    return 'tables/gf.scaled.tsv'

def compute_diff(table):
    import pandas as pd
    import numpy as np
    cross = np.dot(table.values.T, table.values)
    tn = np.array([np.dot(table.values.T[i], table.values.T[i]) for i in range(table.shape[1])])
    tn = np.array([np.dot(table.values.T[i], table.values.T[i]) for i in range(table.shape[1])])
    diff = np.atleast_2d(tn).T + np.atleast_2d(tn) - 2*cross
    return pd.DataFrame(diff, index=table.columns, columns=table.columns)

@TaskGenerator
def compute_dists(tablefile):
    import pandas as pd
    import numpy as np
    table = pd.read_table(tablefile, index_col=0)
    table /= table.sum()
    diff = compute_diff(table)
    diff.to_csv('tables/gf.dist.tsv', sep='\t')
    table = np.log10(table +1e-6)
    diff = compute_diff(table)
    diff.to_csv('tables/gf.logeuc.dist.tsv', sep='\t')


gfiles = CachedFunction(glob, 'outputs.txt/*.txt.gz')
gfiles.sort()
for us in blocks_of(gfiles, 256):
    project_files(us)


barrier()

max_values = []
for us in blocks_of(gfiles, 256):
    max_values.append(maximum_of(us))
max_values_final = global_max(max_values)

active = extract_active(max_values_final, 1e-3)

non_unique = [g for g in gfiles if '.unique.' not in g]
table = []
for us in blocks_of(non_unique, 256):
    table.append(load_submatrix(us, active))

table = save_final_table(table)
compute_dists(table)
