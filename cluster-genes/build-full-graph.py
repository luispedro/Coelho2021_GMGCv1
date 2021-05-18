#!/usr/bin/env python3

# This script assumes the following project structure (auto created if missing)
# ./data
# ./data/split
# ./logs
# ./results
#

from __future__ import print_function
from itertools import product
import multiprocessing as mp
import gzip
from jug import TaskGenerator, bvalue, value, Task
from smart_open import smart_open
import pathlib
from pathlib import Path
from subprocess import run
import os
import re
from ncpus import get_ncpus
import utils
from jug.hooks import exit_checks
from jug.utils import sync_move

mpctx = mp.get_context('spawn')

exit_checks.exit_if_file_exists('jug.exit.marker')
if 'MAX_JUG_TASKS' in os.environ:
    exit_checks.exit_after_n_tasks(int(os.environ['MAX_JUG_TASKS']))

exec(open('config.py').read())
for fname in INPUTS:
    if not str(fname).endswith('.fna'):
        raise ValueError("Input file names MUST end in .fna")

BIN = Path("/g/scb2/bork/mocat/software")

N_CPUS = get_ncpus()
TMPDIR = os.environ.get("TMPDIR", "/tmp")

MILLION_BPS_PER_CHUNK = 10 * 400

LOGS = Path("logs")
RESULTS = Path("results")
DATA = Path("data")
SPLIT = DATA / "split"

DNA_SUFFIX = ".fna"
PROTEIN_SUFFIX = ".faa"

DIAMOND = BIN / "diamond" / "0.8.36" / "diamond"
FNA2FAA = BIN / "fna2faa" / "0.0.2" / "fna2faa"

@TaskGenerator
def value_after(val, after):
    return val

def logfile_from_path(*paths, prefix="path"):
    path = "_".join(Path(p).stem for p in paths)

    return LOGS / "{}_{}.log".format(prefix, path)


@Task
def prechecks():
    """Ensure necessary folders and inputs exist before starting"""

    LOGS.mkdir(exist_ok=True)
    RESULTS.mkdir(exist_ok=True)
    DATA.mkdir(exist_ok=True)
    SPLIT.mkdir(exist_ok=True)
    for fname in INPUTS:
        assert fname.is_file(), "ERROR: {} doesn't exist or is not a file".format(fname)


@Task
def make_outputdirs():
    os.makedirs('outputs/', exist_ok=True)
    os.makedirs('outputs/nucleotide.checked', exist_ok=True)
    os.makedirs('outputs/nucleotide.redundant', exist_ok=True)
    os.makedirs('outputs/union.find', exist_ok=True)
    os.makedirs('outputs/splits', exist_ok=True)
    os.makedirs('outputs/catalog', exist_ok=True)

def run_paths(cmd, **kw):
    import subprocess
    subprocess.run(list(map(str, cmd)), **kw)


@TaskGenerator
def dna_to_protein(dna, force_update=False):
    protein = dna.with_suffix(PROTEIN_SUFFIX)

    if protein.is_file() and not force_update:
        print("WARNING: Protein sequence already exists. Skipping translation")
        return protein

    with protein.open('wb') as out:
        run_paths([FNA2FAA, dna], stdout=out, check=True)

    return protein


@TaskGenerator
def split_sequences(seq, suffix):
    seq_split = (SPLIT / seq.name).with_suffix(suffix)
    log_path = logfile_from_path(seq, prefix="sequence_fastasplit")

    with log_path.open('wb') as log:
        run_paths(['./bin/SplitBlocks', seq, seq_split, '-b', MILLION_BPS_PER_CHUNK * 1000*1000],
                  stdout=log, stderr=log, check=True)

    return list(SPLIT.glob("{}.*{}".format(seq.stem, suffix)))


@TaskGenerator
def make_diamond_db(chunk):
    log_path = logfile_from_path(chunk, prefix="diamond_makedb")

    with log_path.open('wb') as log:
        run_paths([DIAMOND, "makedb", "--in", chunk, "--db", chunk],
                  stdout=log, stderr=log, check=True)

    return chunk


def run_diamond(chunk, chunkdb, outdir):
    out =  pathlib.PurePath("{}/{}___{}.txt".format(outdir, chunk.stem, chunkdb.stem))
    print("IN RUN_DIAMOND", chunk, chunkdb, out)

    if chunkdb.endswith('.dmnd'):
        chunkdb = chunkdb[:-len('.dmnd')]
    with tempfile.NamedTemporaryFile(dir=out.parent, delete=False) as t_out:
        try:
            t_out.close()
            done = False
            cmd = [DIAMOND, "blastx",
                        "-p", str(N_CPUS),
                        "-f", "6",
                        "--query", chunk,
                        "--db", chunkdb,
                        "--out", str(t_out.name),
                        "-t", TMPDIR,
                        # The following two options make the result faster (while using more temp disk space):
                        "--block-size", "8.0",
                        "--index-chunks", "1",
                        ]

            run_paths(cmd, check=True)
            sync_move(str(t_out.name), str(out))
            done = True
        except:
            if not done:
                os.unlink(t_out.name)
            raise
    return out

@TaskGenerator
def run_bzip2(f):
    from os import path
    if not path.exists(f) and path.exists(str(f) + '.bz2'):
        return str(f) + '.bz2'
    import subprocess
    subprocess.run(['bzip2', f], check=True)
    return str(f) + '.bz2'


def is_single_line_fasta(fafile):
    for i,line in enumerate(open(fafile)):
        if i % 2 == 0:
            if line[0] != '>':
                return False
        else:
            if line[0] == '>':
                return False
    return True

@TaskGenerator
def create_dhi_file(fafile):
    from index_fasta import check_max_header_len, create_index
    ofile = str(fafile) + '.dhi'
    n_seqs, key_length = check_max_header_len(input_file=fafile)
    create_index(input_file=fafile, output_file=ofile, key_length=(key_length+1), n_seqs=None)
    return fafile


def realign_pairs(fafile0, fafile1, fname):
    '''Check nucleotide alignments in `fname`'''
    import pair_compare
    from functools import partial
    import os
    from jug.utils import sync_move

    ofname = pathlib.PurePath('outputs/nucleotide.checked') / (fname.name + '.gz')

    redundant_ofname = str(ofname).replace('nucleotide.checked', 'nucleotide.redundant')
    fafile1 = fafile1.with_suffix(".fna")
    fafile0 = str(fafile0)
    fafile1 = str(fafile1)
    with gzip.open(str(ofname) + 'tmp', 'wt') as output, \
            gzip.open(redundant_ofname + 'tmp', 'wt') as redundant_output, \
            utils.maybe_bz2_open(str(fname), 'rt') as ifile, \
            mpctx.Pool(N_CPUS) as p:
        redundant_output.write("gene\trelationship\trepresentative\n")
        grouped = utils.groups_of(ifile, 1024)
        for qs in p.imap(partial(pair_compare.compare_lines, fafile0, fafile1), grouped):
            for rel, g0, g1 in qs:
                if rel == 'M' or rel == 'W':
                    if rel == 'W':
                        g0,g1 = g1,g0
                    output.write(f"{g0}\tR\t{g1}\n")
                elif rel == 'E':
                    output.write(f"{g0}\tR\t{g1}\n")
                    output.write(f"{g1}\tR\t{g0}\n")
                else:
                    if rel == 'D':
                        g0,g1 = g1,g0
                        rel = 'C'
                    elif rel == '=' and g0 > g1:
                        # keep alphabetically smaller gene (arbitrary criterion)
                        g0,g1 = g1,g0
                    redundant_output.write(f'{g0}\t{rel}\t{g1}\n')
    sync_move(str(ofname) + 'tmp', ofname)
    sync_move(redundant_ofname + 'tmp', redundant_ofname)
    os.unlink(str(fname))
    pair_compare.clear_cache()
    return redundant_ofname

@TaskGenerator
def align_pairs(dna, prot_db):
    diamond_out = run_diamond(dna, prot_db, os.environ['LARGE_TMPDIR'])
    return realign_pairs(dna, prot_db, diamond_out)

prot_dbs = []
dna_chunks = []

for ifile in INPUTS:
    splits = bvalue(split_sequences(ifile, DNA_SUFFIX))
    splits.sort()
    prot_chunks = [dna_to_protein(ch) for ch in splits]

    prot_dbs.extend([make_diamond_db(p) for p in prot_chunks])
    dna_chunks.extend([create_dhi_file(d) for d in splits])

aligned = []
for dna, prot_db in product(dna_chunks, prot_dbs):
    aligned.append(align_pairs(dna, prot_db))
