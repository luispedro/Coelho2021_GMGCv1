from collections import namedtuple
from pathlib import PurePath
import gzip
import re
from glob import glob
from jug import TaskGenerator, CachedFunction
from jug import mapreduce

BIN_DIR = '/g/scb2/bork/orakov/GMGC/Projects/bins/'
ASSEMBLED_TEMPLATE = '/g/bork1/coelho/DD_DeCaF/genecats/sources/assemble-genes/orfs/{sample}-assembled.mgm.gz'

GeneTypes = namedtuple('GeneTypes', ['binned_genes', 'unbinned_genes', 'filtered_out_genes'])

@TaskGenerator
def gene2gmbc(sample):
    import pandas as pd
    meta = pd.read_table('cold/GMBC10.meta.tsv', index_col=0)
    bin2gmbc10 = meta['genome'].reset_index().set_index('genome').squeeze().to_dict()

    active = {}
    for b in glob(BIN_DIR + sample + '/*.gz'):
        if 'unbinned' in b:
            bname = '{}_unbinned'.format(sample)
        else:
            bname = bin2gmbc10[b[len(BIN_DIR)+len(sample)+1:-len('.fa.gz')]]
        for line in gzip.open(b, 'rt'):
            if line[0] == '>':
                active[line[1:-1]] = bname
    ofile = f'gene2gmbc.partials/{sample}.txt'
    with open(ofile, 'wt') as out:
        assembled = ASSEMBLED_TEMPLATE.format(sample=sample)
        for line in gzip.open(assembled, 'rt'):
            if line[0] == '>' and '_nt|' in line:
                m = re.match(r'>gene_(\d+)\|GeneMark.hmm\|\d+_nt\|[-+].*>(k.*) flag=.*', line)
                [g,c] = m.groups()
                if c in active:
                    n_g = f'{sample}_{g}'
                    out.write(f'{n_g}\t{active[c]}\n')
    return ofile

@TaskGenerator
def count_genes(sample):
    binned_contig = set()
    unbinned_contig = set()

    for b in glob(BIN_DIR + sample + '/*.gz'):
        active = (unbinned_contig if 'unbinned' in PurePath(b).name else binned_contig)
        for line in gzip.open(b, 'rt'):
            if line[0] == '>':
                active.add(line[1:-1])
    unbinned_genes = 0
    binned_genes = 0
    filtered_out_genes = 0
    assembled = ASSEMBLED_TEMPLATE.format(sample=sample)
    for line in gzip.open(assembled, 'rt'):
        if line[0] == '>' and '_nt|' in line:
            m = re.match(r'>gene_\d+\|GeneMark.hmm\|\d+_nt\|[-+].*>(k.*) flag=.*', line)
            [g] = m.groups()
            if g in binned_contig:
                binned_genes += 1
            elif g in unbinned_contig:
                unbinned_genes += 1
            else:
                filtered_out_genes += 1
    return GeneTypes(binned_genes=binned_genes, unbinned_genes=unbinned_genes, filtered_out_genes=filtered_out_genes)
            
@TaskGenerator
def concatenate_counts(counts):
    import pandas as pd
    return pd.DataFrame([
            pd.Series({
                'binned_genes': c.binned_genes,
                'unbinned_genes': c.unbinned_genes,
                'filtered_out_genes': c.filtered_out_genes,
            }) for c in counts])


@TaskGenerator
def count_mags(samples):
    import numpy as np
    nrs = []
    for s in samples:
        nrs.append(len(glob(BIN_DIR+s+'/*.bin*.fa.gz'))-1)
    return np.array(nrs)


SAMPLES = CachedFunction(glob, BIN_DIR + '*')
SAMPLES.sort()
SAMPLES = [PurePath(s).name for s in SAMPLES]
counts = []
for sample in SAMPLES:
    counts.append(count_genes(sample))

gene2gmbc_partials = mapreduce.map(gene2gmbc, SAMPLES, map_step=32)

final = concatenate_counts(counts)
mags = count_mags(SAMPLES)
