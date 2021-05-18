from jug.utils import jug_execute
from glob import glob
from jug import TaskGenerator, CachedFunction

DIAMOND = '/g/scb2/bork/mocat/software/diamond/0.8.36/diamond'
DIAMOND_OPTS = ['-c', '1', '-b', '4.0', '-t', '/local/coelho/tmp']
THREADS = 48

## wget 'ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.xml.gz'
## wget 'ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_trembl.xml.gz'

@TaskGenerator
def makedb(ifile):
    jug_execute.f([DIAMOND, 'makedb', '--threads', str(THREADS), '--in', ifile, '-d', ifile])
    return ifile

@TaskGenerator
def run_diamond(split, db):
    ofile = split.replace('splits/', 'output/').replace('.faa', '.m8')
    jug_execute.f([DIAMOND, 'blastp', '--threads', str(THREADS), '-d', db, '-q', split, '-o', ofile] + DIAMOND_OPTS)
    return ofile

def load_taxid():
    taxid = {}
    for line in open('sprot.taxid'):
        g,t = line.split()
        taxid[g] = t

    for line in open('trembl.taxid'):
        g,t = line.split()
        taxid[g] = t
    return taxid

def load_pairs(ifile):
    for line in open(ifile):
        tokens = line.split('\t')
        yield (tokens[0], tokens[1])

_taxid = None

@TaskGenerator
def reprocess(ifile):
    global _taxid
    if _taxid is None:
        _taxid = load_taxid()
    from taxonomic import ncbi
    from itertools import groupby
    n = ncbi.NCBI()
    ofile = ifile + '.taxid'
    with open(ofile, 'wt') as output:
        for g,ms in groupby(load_pairs(ifile), lambda tk: tk[0]):
            for _,m in ms:
                m = m.split('|')[-1]
                if m in _taxid:
                    t = _taxid[m]
                    k = n.at_rank(t, 'superkingdom')
                    output.write(f"{g}\t{k}\n")
                    break
            else:
                output.write(f"{g}\tno_match\n")
    return ofile

@TaskGenerator
def postprocess(taxids):
    from collections import Counter
    import pandas as pd

    rename = {}
    for line in open('cold/GMGC10.rename.table.txt'):
        new,old = line.split()
        rename[old] = new
    c = Counter()
    for i,fname in enumerate(sorted(taxids)):
        f = pd.read_table(fname, header=None, index_col=0, squeeze=True)
        f.rename(index=rename, inplace=True)
        c.update(f)
        pd.DataFrame({'kindgom' : f}).to_csv(fname + '.renamed', sep='\t', header=False)
        print(f'Done {i}')
    return c

splits = CachedFunction(glob, 'splits/*.faa')
splits.sort()

db = makedb('data/uniprot_sprot_and_trembl.fasta')
partials = []
for s in splits:
    p = run_diamond(s, db)
    partials.append(reprocess(p))

counts = postprocess(partials)

