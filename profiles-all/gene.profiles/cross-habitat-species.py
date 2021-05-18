import pandas as pd
from jug import TaskGenerator, CachedFunction
from glob import glob
from jug.utils import identity
uniques = CachedFunction(glob, 'outputs.txt/*unique.txt.gz')
uniques.sort()



@TaskGenerator
def get_gene_list(sp):

    import sqlite3
    c_rename = sqlite3.connect('cold/GMGC10.rename.table.sqlite3')
    c_taxonomic = sqlite3.connect('cold/GMGC10.taxonomic.reconciliation.sqlite3')
    r = c_taxonomic.execute('SELECT gene FROM taxonomic WHERE name = ?', [sp])
    genes = r.fetchall()
    genes = [g for (g,) in genes]
    old_genes = [c_rename.execute('SELECT old_name FROM rename WHERE new_name = ?', [g]).fetchone()[0] for g in genes]
    return old_genes

@TaskGenerator
def active(u, sgenes):
    return pd.read_table(u, index_col=0, squeeze=True).reindex(sgenes).dropna()

@TaskGenerator
def len_(a):
    return len(a)


CANDIDATES = [
        'Lyngbya confervoides',
        'Acidovorax ebreus',
        'Methylobacterium oryzae',
        ]

final = {}
for sp in CANDIDATES:
    genes = get_gene_list(sp)

    detected = {}
    ndetected = {}
    for u in uniques:
        detected[u] = active(u, genes)
        ndetected[u] = len_(detected[u])

    final[sp] = identity(ndetected)
