from os import makedirs, path, environ
from jug import Task, TaskGenerator, barrier
from jug.utils import jug_execute
from glob import glob
import subprocess
from jug.hooks import exit_checks

exit_checks.exit_if_file_exists('jug.exit.marker')
if 'MAX_JUG_TASKS' in environ:
    exit_checks.exit_after_n_tasks(int(environ['MAX_JUG_TASKS']))


import ncpus

IS_FILE_LIST = False
try:
    exec(open('./config.py').read())
except:
    from sys import stderr,exit
    stderr.write('''
Please write a configuration file 'config.py' where the following three variables are set:

INPUT
OUTPUT
TAG
''')
    exit(1)

@Task
def make():
    jug_execute.f(['make'])


@Task
def makeoutdirs():
    makedirs(f'partials.{TAG}/', exist_ok=True)
    makedirs(f'partials.{TAG}/copies', exist_ok=True)
    makedirs(f'partials.{TAG}/splits', exist_ok=True)
    makedirs(f'partials.{TAG}/filtered', exist_ok=True)

@TaskGenerator
def value_after(val, after):
    return val

@TaskGenerator
def sort_size(ifile, is_file_list):
    from os import path
    base = path.basename(ifile)
    ofile = f'partials.{TAG}/{base}.sorted.fna'
    args = [('-F' if is_file_list else '-i'), ifile
            ,'-o', ofile
            ,'-j', str(ncpus.get_ncpus())]
    jug_execute.f(['./bin/SortSizes'] + args)
    return ofile

def extract_block_size(fname):
    import re
    m  = re.match(f'^partials.{TAG}/splits/block\.(\d+)\.fna$', fname)
    return int(m.group(1), 10)

@TaskGenerator
def find_overlaps(p, extra, oname):
    import tempfile
    import os
    n = tempfile.NamedTemporaryFile(mode='wt', delete=False)
    try:
        for e in extra:
            n.write(f'{e}\n')
        n.flush()
        n.close()
        subprocess.check_call([
            './bin/FindExactOverlaps',
            '-2',
            '-o', oname,
            '-i', p,
            '-e', n.name,
            '-j', str(ncpus.get_ncpus()),
            ])
    finally:
        os.unlink(n.name)

@TaskGenerator
def remove_duplicates(p, dups, oname):
    import tempfile
    import os
    n = tempfile.NamedTemporaryFile(mode='wt', delete=False)
    try:
        for e in dups:
            for d in open(e):
                d = d.split()[0]
                n.write(f'{d}\n')
        n.flush()
        n.close()
        subprocess.check_call([
            './bin/Remove',
            '-i', p,
            '-o', oname,
            '-d', n.name,
            ])
    finally:
        os.unlink(n.name)
    return oname

def block(xs, n):
    while xs:
        yield xs[:n]
        xs = xs[n:]

@TaskGenerator
def concatenate_files(partials, oname):
    with open(oname, 'wb') as output:
        for p in partials:
            with open(p, 'rb') as pinput:
                while True:
                    chunk = pinput.read(8192)
                    if not chunk:
                        break
                    output.write(chunk)

@TaskGenerator
def sort_size_files(filelist, ofile):
    from os import path, unlink
    import tempfile
    n = tempfile.NamedTemporaryFile(mode='wt', delete=False)
    for f in filelist:
        n.write(f)
    n.close()
    args = ['-F', n.name
            ,'-o', ofile
            ,'-j', str(ncpus.get_ncpus())]
    jug_execute.f(['./bin/SortSizes'] + args)
    unlink(n.name)
    return ofile

@TaskGenerator
def merge_blocks(filelist, ofile):
    from os import path, unlink
    import tempfile
    n = tempfile.NamedTemporaryFile(mode='wt', delete=False)
    for f in filelist:
        n.write(f'{f}\n')
    n.close()
    args = ['-F', n.name
            ,'-o', ofile
            ,'-j', str(ncpus.get_ncpus())]
    jug_execute.f(['./bin/MergeSizes'] + args)
    unlink(n.name)
    return ofile

if IS_FILE_LIST:
    ifiles = [line for line in open(INPUT)]
    if len(ifiles) > 400:
        blocks = list(block(ifiles, 200))
        sorted_blocks = []
        for i,b in enumerate(blocks):
            sorted_blocks.append(sort_size_files(b, f'partials.{TAG}/sorted.block.{i}.fna'))
        base = path.basename(INPUT)
        ofile = f'partials.{TAG}/{base}.sorted.fna'
        input_sorted = merge_blocks(sorted_blocks, ofile)
    else:
        input_sorted = sort_size(INPUT, IS_FILE_LIST)
else:
    input_sorted = sort_size(INPUT, IS_FILE_LIST)

ofile_exact = f'partials.{TAG}/exact100.filtered.fna'
exact_copy_files = f'partials.{TAG}/copies/exact_0.txt'
r = jug_execute(['./bin/RemoveRepeats', input_sorted, '-o', ofile_exact, '-d', exact_copy_files])
jug_execute(['./bin/SplitBlocks', ofile_exact, '-b', str(300*10*1000*1000),'-d', f'partials.{TAG}/splits/block'], run_after=r)

barrier()
partials = glob(f'partials.{TAG}/splits/*.fna')
partials.sort(key=extract_block_size)

allcopies = [exact_copy_files]
final = []
for i,p0 in enumerate(partials):
    oname = f'partials.{TAG}/copies/{i}_self.txt'
    r = jug_execute(['./bin/FindExactOverlaps', p0, '-o', oname])
    copies = [value_after(oname, after=r)]
    others = partials[i+1:]
    for j,chunk in enumerate(block(others, 8)):
        oname=f'partials.{TAG}/copies/{i}_{j}.txt'
        fo = find_overlaps(p0, chunk, oname)
        copies.append(value_after(oname, after=fo))
    final.append(
            remove_duplicates(p0, copies, p0.replace('/splits/', '/filtered/')))
    allcopies.extend(copies)

concatenate_files(final, OUTPUT)
concatenate_files(allcopies, OUTPUT_COPIES)
