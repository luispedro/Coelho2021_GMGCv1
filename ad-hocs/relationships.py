from jug import Task, TaskGenerator, CachedFunction
from jug.utils import jug_execute
from itertools import groupby
from safeout import safeout
from glob import glob

PSORT = '/g/bork3/home/coelho/work/MOCAT/public/bin/psort'
TEMPDIR = '/alpha/local/coelho/tmp/'
NCPUS = 24
REL_FILES = CachedFunction(glob, '../cluster-genes/outputs/relationships.complete/*.gz')
REL_FILES.sort()


@TaskGenerator
def filter_relationships(eqs, oname):
    jug_execute.f(['./bin/FilterTriplets',
                    '-c', '../cluster-genes/outputs/catalog.complete.txt',
                    '-i', eqs,
                    '-v', '-t8',
                    '-o', oname,])
    return oname


@TaskGenerator
def cat_gzips(gzs, oname):
    import gzip
    from glob import glob
    with open(oname, 'wb') as output:
        for g in gzs:
            with gzip.open(g, 'rb') as ifile:
                while True:
                    block = ifile.read(8192)
                    if not block:
                        break
                    output.write(block)
    return oname


@TaskGenerator
def run_sort(ifile, ofile, extra=[]):
    ifiles = (ifile if type(ifile) == list else [ifile])
    jug_execute.f([PSORT, '--batch-size=32', '-S324G', '-T', TEMPDIR, '--parallel={}'.format(NCPUS)] + ifiles + ['-o', ofile] + extra)
    return ofile


@TaskGenerator
def sort_merged(merged):
    oname = merged + '.sorted3'
    return run_sort.f(merged, oname, ['-k', '3'])

@TaskGenerator
def uniq(fname):
    oname = fname + '.uniq'
    jug_execute.f(['uniq', fname, oname])
    return oname

def combine_rels(r0, r1):
    assert r0 in 'C='
    if r1 == 'R':
        return r1
    if r1 == 'C':
        return 'C'
    if r1 == '=':
        return r0
    raise NotImplementedError("??")

@TaskGenerator
def join_relationships(sorted_copies, sorted_relationships):
    import subprocess
    oname = 'big-temp/GMGC.relationships.joined.txt'
    subprocess.check_call(['./bin/MergeRelationships',
                            '-1', sorted_copies,
                            '-2', sorted_relationships,
                            '-o', oname, '-t8', '-v'])
    return oname

@TaskGenerator
def jug_shell(cmd):
    import subprocess
    subprocess.check_call(cmd, shell=True)

@TaskGenerator
def value_after(v, after):
    return v


@TaskGenerator
def cut1uniq(ifile, ofile):
    prev = 'x'
    n = 0
    with safeout(ofile, 'wt') as output:
        for line in open(ifile):
            f1 = line.split()[0]
            if f1 != prev:
                output.write(f'{f1}\n')
                prev = f1
                n += 1
    print(f"{n} genes explained")
    return n

@TaskGenerator
def get_unexplained(inames, rels, extra, oname):
    import pandas as pd
    explained = set(line.strip() for line in open(rels))
    for ef in extra:
        for line in open(ef):
            explained.add(ef.strip())
    with safeout(oname, 'wt') as output:
        for line in open(inames):
            if line.strip() not in explained:
                output.write(line)
    return oname

def split(xs, N):
    return [xs[i::N] for i in range(N)]


def compress(ells):
    first,second = ells
    a0,r0,b0 = first.split()
    a1,r1,b1 = second.split()
    assert a0 == a1
    assert b0 == b1
    assert r0 == 'C'
    assert r1 == 'R'
    return [first]

@TaskGenerator
def filter_RC(ifile, oname):
    from itertools import groupby
    with safeout(oname, 'wt') as output:
        for k,ells in groupby(open(ifile), lambda line: (line.split()[0], line.split()[2])):
            ells = list(ells)
            if len(ells) > 1:
                ells = compress(ells)
            for el in ells:
                output.write(el)
    return oname


sorted_copies = run_sort(['../redundant100/data/GMGC.meta.copies.txt', '../redundant100/data/freeze12.copies.txt'],
                            'big-temp/GMGC.meta.copies.sorted3.txt',
                             ['-k', '3'])

eq1 = filter_relationships('../redundant100/data/GMGC.meta.copies.txt', 'big-temp/equality1.txt')
eq2 = filter_relationships('../redundant100/data/freeze12.copies.txt', 'big-temp/equality2.txt')
rels = [cat_gzips(gz, 'big-temp/all.by.all.{}.txt'.format(i)) for i,gz in enumerate(split(REL_FILES, 12))]

sorted_relationships = run_sort([eq1, eq2] + rels, 'big-temp/GMGC.relationships.sorted1.txt', ['-u'])

merged = join_relationships(sorted_copies, sorted_relationships)
all_files = [eq1, eq2, merged] + rels
u = run_sort(all_files, 'big-temp/GMGC.all.relationships.step1.sorted.uniq', ['-u'])

# At this point, several genes are still unexplained. Recover them

@TaskGenerator
def get_uniq_f1(fs, oname):
    import subprocess
    with safeout(oname, 'wb') as output:
        p1 = subprocess.Popen(['cut', '-f1'] + fs, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['uniq'], stdin=p1.stdout, stdout=output.tfile.file)
        p1.stdout.close()
        p1.wait()
        p2.wait()
        return oname

unexplained = get_unexplained(
        'big-temp/all.input.names.txt',
        get_uniq_f1([u], 'big-temp/all.step1.explained.txt'),
        ['big-temp/short.genes.txt.sorted',
        'short.Fr12.genes.txt.sorted',
        '../cluster-genes/outputs/catalog.complete.txt'],
        oname='big-temp/unexplained.step1.txt')

ucopies = 'big-temp/unexplained.step1.copies.txt'
ucopies = value_after(ucopies,
                jug_execute(['./bin/FilterTriplets'
                        ,'-i', sorted_copies
                        ,'-o', ucopies,
                        '-2', '-c', unexplained
                        ,'-v', '-t8']))

reexplain = 'big-temp/re-explain.txt'
reexplain = value_after(reexplain,
        jug_execute(['./bin/MergeRelationships', '-1', ucopies, '-2', u, '-v', '-t8', '-o', reexplain]))
reexplain1 = run_sort(reexplain, 'big-temp/re-explain.sorted1.txt')

s1 = run_sort([reexplain1, u], 'big-temp/GMGC.all.relationships.sorted1.RC.txt', ['--merge'])
s1 = filter_RC(s1, 'big-temp/GMGC.all.relationships.sorted1.txt')

s3 = run_sort(s1, 'big-temp/GMGC.all.relationships.sorted3.txt', ['-k3'])

