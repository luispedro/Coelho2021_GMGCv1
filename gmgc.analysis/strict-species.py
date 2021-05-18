from itertools import groupby
from taxonomic import lca
from taxonomic import ncbi
n = ncbi.NCBI()

def read_relationships():
    for line in open('cold/GMGC.relationships.txt'):
        a,_,b = line.rstrip().split()
        yield a,b
        
fr12name = {}
for line in open('cold/freeze12.rename.table'):
    i,g,_ = line.strip().split('\t',2)
    fr12name[i] = g

rename = {}
for line in open('cold/GMGC10.rename.table.txt'):
    new,old = line.rstrip().split()
    rename[old] = new

with open('GMGC10.inner.taxonomic.map', 'wt') as output:
    for g,origs in groupby(read_relationships(), lambda a_b:a_b[1]):
        fr_genes = [o for o,_ in origs if o.startswith('Fr12_')]
        if fr_genes:
            classif = lca.lca([n.path(fr12name[f].split('.')[0]) for f in fr_genes])
            classif = classif[-1]
            sp = n.at_rank(classif, 'species')
            if sp:
                output.write('\t'.join([g, rename[g], classif, sp]))
                output.write('\n')
