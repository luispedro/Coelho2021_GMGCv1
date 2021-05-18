from datetime import datetime
import gzip
import multiprocessing
from glob import glob
from utils import groups_of
from os import path, environ
from jug import TaskGenerator, CachedFunction, iteratetask
from jug.utils import sync_move
import diskhash
import gzip


from jug import Task, TaskGenerator, barrier, CachedFunction
from jug.utils import jug_execute, identity
from jug import set_jugdir
import os


import tempfile
import pair_compare
import utils
from ncpus import get_ncpus
from jug.hooks import exit_checks

set_jugdir('build-full-graph.jugdata')

import importlib
graph = importlib.import_module('build-full-graph')

TAG = graph.TAG
INPUTS = graph.INPUTS
OUTPUT = graph.OUTPUT
N_CPUS = get_ncpus()

# This is a hack, but it is a significant speedup
_catalog_set_precomputed = None

@TaskGenerator
def filter_relationships(redfile, catfile, tag):
    from os import path, makedirs, unlink
    import safeout
    global _catalog_set_precomputed
    makedirs(f'outputs/relationships.{tag}', exist_ok=True)
    oname = path.join(f'outputs/relationships.{tag}', path.basename(redfile))
    print(f'Starting generating {oname}')
    wrote = False
    if _catalog_set_precomputed is None:
        catalog = set()
        for line in open(catfile):
            catalog.add(line.strip())
        _catalog_set_precomputed = catalog
    else:
        catalog = _catalog_set_precomputed
    print(f'Loaded catalog')
    with safeout.safeout(oname, 'wb') as fo:
        with gzip.open(fo, 'wt') as ofile:
            for ifile in [redfile, redfile.replace('redundant', 'checked')]:
                print(f'Processing {ifile}...')
                if not path.exists(ifile):
                    continue
                for line in gzip.open(ifile, 'rt'):
                    _,_,g = line.strip().split('\t')
                    if g in catalog:
                        ofile.write(line)
                        wrote = True
    if wrote:
        print(f'Wrote {oname}')
        return oname
    print(f'{oname} was empty! REMOVING')
    unlink(oname)

@TaskGenerator
def generate_header_file(tag, ifiles):
    import diskhash
    import shutil
    dhname = f'data/{tag}.headerix.dht'
    tfile = tempfile.NamedTemporaryFile(delete=False)
    tfile.close()
    try:
        dh = diskhash.Str2int(tfile.name, 23, 'w')
        oheadname = f'data/{tag}.headers'
        ix = 0
        with open(oheadname, 'wt') as oheaders:
            for f in ifiles:
                with open(f) as ifile:
                    for line in ifile:
                        if line[0] == '>':
                            first = line.strip().split()[0]
                            first = first[1:]
                            dh.insert(first, ix)
                            first = first + ''.join([' ' for _ in range(23-len(first))])
                            oheaders.write(f'{first}\n')
                            ix += 1
                            if ix % 1000000 == 0:
                                print("Indexed {}m genes".format(ix/1000/1000))
        print("Finished indexing")
        del dh # close the file
        shutil.copy(tfile.name, dhname + 'tmp')
        sync_move(dhname + 'tmp', dhname)
    finally:
        os.unlink(tfile.name)

    return ix,dhname, oheadname

ix, dhname, oheadname = iteratetask(
        generate_header_file(TAG, INPUTS),
        3)


def value_after(val, after):
    return identity( [val, after] )[0]

@TaskGenerator
def collect_redundant(rs, oname):
    import pandas as pd
    import safeout
    from os import path
    redundant = set()
    for r in rs:
        if not path.exists(r):
            continue
        r = pd.read_table(r, usecols=[0])
        redundant.update(r.T.iloc[0])
    with safeout.safeout(oname, 'wt') as output:
        for r in redundant:
            output.write(f'{r}\n')
    return oname


@TaskGenerator
def jug_shell(c, run_after):
    import subprocess
    return subprocess.check_call(c, shell=True)


@TaskGenerator
def save_file_list(file_list, i, tag):
    file_list_name = f'outputs/uf.{tag}/partial-file-list.{i}.txt'
    with open(file_list_name, 'wt') as output:
        for f in file_list:
            output.write(f'{f}\n')
    return file_list_name


@TaskGenerator
def partial_uf(filelist, oname, dhname, blacklist):
    print("partial_uf()")
    if environ.get('MAKE_LOCAL_COPIES') == '1':
        print("partial_uf(): making a temporary copy")
        dhname = utils.make_temp_copy(dhname, environ['TMPDIR'])
    args = ['./bin/UnionFind', '-o', oname, '-d', dhname, '-t', str(N_CPUS), '-v', '--black-list', blacklist]
    for f in filelist:
        args.extend(['-i', f])
    print(' '.join(args))
    jug_execute.f(args)
    return oname

@TaskGenerator
def bin_partial_uf(filelist, oname, dhname, full=False):
    args = ['./bin/UnionFind', '-o', oname, '-t', str(N_CPUS), '-v', '-d', dhname]
    for f in filelist:
        args.extend(['-b', f])
    if full:
        args.append('-f')
    jug_execute.f(args)
    return oname

@TaskGenerator
def annotate_triplets(filelist, oname, blacklist, dhname, uf):
    if environ.get('MAKE_LOCAL_COPIES') == '1':
        print("annotate_triplets(): making a temporary copy")
        dhname = utils.make_temp_copy(dhname, environ['TMPDIR'])
        uf = utils.make_temp_copy(uf, environ['TMPDIR'])

    args = ['./bin/AnnotateTriplets', '-o', oname, '-t', str(N_CPUS), '-v', '-d', dhname, '-u', uf, '--blacklist', blacklist]
    for f in filelist:
        args.extend(['-i', f])
    jug_execute.f(args)
    return oname

@TaskGenerator
def red_to_checked(ps):
    return [p.replace('redundant', 'checked') for p in ps]


@TaskGenerator
def sort_reorder_triplets(iname):
    import numpy as np
    from os import unlink
    with open(iname, 'rb') as ifile:
        data = np.frombuffer(ifile.read(), np.uint32).reshape((-1,3)).copy()
    data = data.T[[0,2,1]].T
    data = data[np.lexsort(data.T[::-1])]
    if not data.flags['C_CONTIGUOUS']:
        data = data.copy()

    oname = iname+'.reordered.sorted'
    with open(oname, 'wb') as output:
        assert data.flags['C_CONTIGUOUS']
        output.write(data.data)
    unlink(iname)
    return oname

@TaskGenerator
def merge_triplets(trips, oname):
    args = ['./bin/MergeTriplets', '-v', '-o', oname]
    for t in trips:
        args.extend(['-i', t])
    jug_execute.f(args)
    return oname

@TaskGenerator
def merge_triplets_pp(trips, oname):
    args = ['./bin/sort-triplets', 'mergemany']
    for t in trips:
        args.append(t)
    args.append(oname)
    jug_execute.f(args)
    return oname


@TaskGenerator
def find_singletons(uf, oname):
    import numpy as np
    import gzip
    data = gzip.open(uf, 'rb')
    ufdata = np.frombuffer(data.read(), np.uint32).reshape((-1,2))

    counts = np.zeros(len(ufdata))
    for ix in ufdata.T[1]:
        counts[ix] += 1

    sentinel = ufdata.T[0] == ufdata.T[1]
    singletons = (counts == 1)&sentinel
    with open(oname, 'wt') as output:
        for ix in np.where(singletons)[0]:
            output.write(f'{ix}\n')
    return oname

@TaskGenerator
def run_dominant(trips, oname):
    import subprocess
    with open(oname, 'wb') as output:
        subprocess.check_call(['./bin/dominant', trips], stdout=output)
    return oname


@TaskGenerator
def concat_catalog(singletons, others, names, oname):
    names = [line.strip() for line in open(names)]
    catalog = []
    for f in [singletons, others]:
        for line in open(f):
            ix = int(line.strip())
            catalog.append(names[ix])

    catalog.sort()
    with open(oname, 'wt') as output:
        for n in catalog:
            output.write(f'{n}\n')
    return oname


@TaskGenerator
def extract_fasta(fas, catfile, oname):
    import subprocess
    args = ['./bin/RetrieveFastaSeqs', '-k', catfile, '-o', oname]
    for f in fas:
        args.extend(['-i', f])
    jug_execute.f(args)
    return oname

@TaskGenerator
def make_faa(fna):
    oname = fna.replace('fna', 'faa')
    assert oname != fna
    with open(oname, 'wb') as out:
        graph.run_paths([graph.FNA2FAA, fna], stdout=out, check=True)
    return oname



redundant = collect_redundant(graph.aligned, f'outputs/redundant.{TAG}.txt')
partials = []
for i,ps in enumerate(groups_of(graph.aligned, 64)):
    ps = red_to_checked(ps)
    oname = f'outputs/uf.{TAG}/partial.{i}.pairs.gz'
    partials.append(partial_uf(ps, oname, dhname, blacklist=redundant))

repartials = []
for i,ps in enumerate(groups_of(partials, 64)):
    oname = f'outputs/uf.{TAG}/partial.iter2.{i}.pairs.gz'
    repartials.append(bin_partial_uf(ps, oname, dhname))
uf = bin_partial_uf(repartials, f'outputs/uf.{TAG}/uf.full.gz', dhname, full=True)

triplets = []
for i,ps in enumerate(groups_of(graph.aligned, 16)):
    ps = red_to_checked(ps)
    oname = f'outputs/triplets.{TAG}/partials.{i}.triplets'
    trips = annotate_triplets(ps, oname=oname, blacklist=redundant, dhname=dhname, uf=uf)
    trips_sorted = sort_reorder_triplets(trips)
    triplets.append(trips_sorted)


if len(triplets) > 64:
    retrips = []
    for i,ts in enumerate(groups_of(triplets, 64)):
        oname = f'outputs/triplets.{TAG}/partials.merge0.{i}.triplets.reordered'
        retrips.append(merge_triplets(ts, oname))
    triplets = retrips

alltrips = merge_triplets_pp(triplets, f'outputs/triplets.{TAG}/all.triplets.reordered')
singletons = find_singletons(uf, f'outputs/singletons.{TAG}')
others = run_dominant(alltrips, f'outputs/catalog.except_singletons.{TAG}.txt')

cat = concat_catalog(singletons, others, oheadname, f'outputs/catalog.{TAG}.txt')
fna = extract_fasta(INPUTS, cat, f'outputs/catalog.{TAG}.fna')
faa = make_faa(fna)

for r in graph.aligned:
    filter_relationships(r, cat, TAG)
