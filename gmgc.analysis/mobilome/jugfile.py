from jug.utils import jug_execute
from jug import TaskGenerator, barrier
from glob import glob

import ncpus

NCPUS = ncpus.get_ncpus()

@TaskGenerator
def total_bps(fa):
    bps = 0
    for line in open(fa):
        if line[0] != '>': bps += len(line) - 1
    return bps

@TaskGenerator
def split_into(fa, obase, n_blocks, total):
    per_block = n_blocks + total//n_blocks
    jug_execute.f(['./bin/SplitBlocks', '-i', fa, '-o', obase, '-b', str(per_block)])

@TaskGenerator
def run_hmmsearch(ifile, hmm_base):
    oname = ifile.replace('.faa', '.{}.hmmout'.format(hmm_base))
    jug_execute.f(['hmmsearch', '--cpu', str(NCPUS), '-o', '/dev/null', '--tblout', oname, '--cut_ga', 'data_hmm/{}.hmm'.format(hmm_base), ifile])
    return oname


@TaskGenerator
def rename_splits(_):
    from os import rename
    for f in sorted(glob('splits/GMGC.95nr.*.fna')):
        rename(f, f.replace('.fna', '.faa'))

FAFILE = 'genecats.cold/GMGC.95nr.faa'
total = total_bps(FAFILE)
rename_splits(split_into(FAFILE, 'splits/GMGC.95nr.faa.split', 128, total))

barrier()
for hmm_base in ['transposase', 'TRdb_170817']:
    for ifile in sorted(glob('splits/GMGC.95nr.*.faa')):
        run_hmmsearch(ifile, hmm_base)
