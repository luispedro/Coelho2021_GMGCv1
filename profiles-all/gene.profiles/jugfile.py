from jug import TaskGenerator, CachedFunction
import os
from os import path

from jug.hooks.exit_checks import exit_if_file_exists, exit_env_vars

exit_env_vars()
exit_if_file_exists('jug.exit.marker')

def get_sample(f):
    return path.split(f)[-1].split('_')[0]

BASE = '/g/scb2/bork/ralves/projects/genecat/outputs/'
HEADERS_FILE = 'GMGC.95nr.fna.sam.header'

def list_samfiles():
    from glob import  glob
    samfiles = glob(BASE + '/*.iter2.*')
    blacklist = set([get_sample(s) for s in samfiles if s.endswith('.sam.gz')])
    return [s for s in samfiles if s.endswith('minimap.iter2.noseq.sam.xz') and get_sample(s) not in blacklist]


_gene_blacklist= None

def expand_sort(ifile, ofile):
    import lzma
    import subprocess
    from os import unlink
    from tempfile import TemporaryDirectory
    global _gene_blacklist
    if _gene_blacklist is None:
        _gene_blacklist  = set(line.strip() for line in open('redundant.complete.sorted.txt'))
    print("Loaded blacklist with {} elements.".format(len(_gene_blacklist )))

    BUFSIZE = 16*1024*1024
    block = []
    partials = []
    def write_block(out_base):
        nonlocal block
        if not block:
            return
        block.sort()
        out_name = out_base + '/' + str(len(partials)) + '.sam'
        print(f'Writing to {out_name}')
        with open(out_name, 'wt') as output:
            for line in block:
                output.write(line)
            block = []
            partials.append(out_name)
        block = []

    with TemporaryDirectory() as tdir:
        for line in lzma.open(ifile, 'rt'):
            if line[0] == '@':
                continue
            if line.split('\t')[2] in _gene_blacklist :
                continue
            block.append(line)
            if len(block) == BUFSIZE:
                write_block(tdir)
        write_block(tdir)
        subprocess.check_call(['sort', '--merge', '-o', ofile] + partials)


@TaskGenerator
def count1(samfile):
    from ngless import NGLess
    import tempfile

    ofname = 'outputs/{}.txt'.format(get_sample(samfile))
    ofname_u = 'outputs/{}.unique.txt'.format(get_sample(samfile))
    sname = tempfile.NamedTemporaryFile(suffix='.sam', delete=False)
    sname.close()
    try:
        sname = sname.name
        expand_sort(samfile, sname)

        sc = NGLess.NGLess('0.7')
        e = sc.env

        e.sam = sc.samfile_(sname, headers=HEADERS_FILE)
        sc.write_(sc.count_(e.sam, features=['seqname'], multiple='{unique_only}', discard_zeros=True),
                ofile=ofname_u + '.gz')
        sc.write_(sc.count_(e.sam, features=['seqname'], discard_zeros=True),
                ofile=ofname + '.gz')
        sc.run(auto_install=False, ncpus='auto')
        return ofname
    finally:
        os.unlink(sname)

_rename = None
def load_rename_table():
    global _rename
    if _rename is None:
        _rename = {}
        for line in open('/g/bork1/coelho/DD_DeCaF/genecats.cold/GMGC10.rename.table.txt'):
            n, name = line.split()
            _rename[name] = int(n.split('.')[1], 10)
    return _rename

def load_gene2bactNOG():
    gene2bactNOG = {}
    for line in open('cold/annotations/GMGC.95nr.emapper.annotations'):
        tokens = line.strip('\n').split('\t')
        bactNOG = [b for b in tokens[9].split(',') if 'bactNOG' in b]
        if len(bactNOG):
            gene2bactNOG[tokens[0]] = bactNOG[0]
    return gene2bactNOG

def load_gene2ko():
    return load_annotation(6)

def load_gene2highNOG():
    return load_annotation(11)

def load_annotation(ix):
    group = {}
    for line in open('cold/annotations/GMGC.95nr.emapper.annotations'):
        tokens = line.strip('\n').split('\t')
        tok = tokens[ix]
        if len(tok):
            group[tokens[0]] = tok
    return group


_sizes = None
def scale(g, sizes):
    total = g.sum()
    gn = g / sizes[g.index]
    gn *= total /gn.sum()
    return gn

@TaskGenerator
def scale_combine(f):
    f = f + '.gz'
    import pandas as pd
    import numpy as np
    global _sizes
    if _sizes is None:
        _sizes = pd.read_table('tables/GMGC.95nr.sizes', index_col=0, squeeze=True)
    _rename = load_rename_table()
    non_unique = pd.read_table(f, index_col=0, squeeze=True)
    fu = f.replace('.txt.gz', '.unique.txt.gz')
    unique = pd.read_table(fu, index_col=0, squeeze=True)
    unique_scaled = scale(unique, _sizes)
    non_unique_scaled = scale(non_unique, _sizes)
    pasted = pd.DataFrame({'raw_unique': unique, 'raw': non_unique, 'unique_scaled' : unique_scaled, 'scaled' : non_unique_scaled})
    pasted.fillna(0, inplace=True)
    pasted.rename(index=_rename, inplace=True)
    pasted.reset_index(inplace=True)
    pasted['index'] = pasted['index'].astype(np.int32)
    fname = f.replace('.txt.gz', '.feather')
    pasted.to_feather(fname)
    return fname


@TaskGenerator
def compute_totals(counts):
    import pandas as pd
    totals = {}
    for f in counts:
        print(f"Loading {f}")
        sample = f.split('/')[1].split('.')[0]
        f = pd.read_feather(f.replace('txt', 'feather'))
        totals[sample] = pd.Series({'total': f['raw'].values.sum(), 'total_unique': f['raw_unique'].values.sum()})
    return pd.DataFrame(totals)

@TaskGenerator
def save_totals(totals):
    totals.T.astype(int).to_csv('tables/total.tsv', sep='\t')

def prepare_gene2functional(annotation, oname_feather, oname_list):
    import pandas as pd
    rename = load_rename_table()
    gi2func = {}
    for k,v in annotation.items():
        gi2func[rename[k]] = v

    gi = pd.DataFrame({'gi2func':pd.Series(gi2func)})

    funcs = list(set(gi.gi2func.values))
    funcs.sort()
    b2i = {b:i for i,b in enumerate(funcs)}
    gi['ixfunc'] = gi.gi2func.map(b2i)
    with open(oname_list, 'wt') as output:
        for f in funcs:
            output.write(f'{f}\n')
    gi.reset_index(inplace=True)
    gi = gi[['index', 'ixfunc']]
    gi.to_feather(oname_feather)
    return oname_feather, oname_list


@TaskGenerator
def prepare_gene2bactNOG():
    return prepare_gene2functional(load_gene2bactNOG(),
                                    'bactNOGS/gene2bactNOG.feather',
                                    'bactNOGS/bactNOGS.txt')

@TaskGenerator
def prepare_gene2ko():
    return prepare_gene2functional(load_gene2ko(),
                                    'kos/gene2ko.feather',
                                    'kos/kos.txt')
@TaskGenerator
def prepare_gene2bigg():
    return prepare_gene2functional(load_annotation(7),
                                    'biggs/gene2bigg.feather',
                                    'biggs/biggs.txt')
@TaskGenerator
def prepare_gene2highNOG():
    return prepare_gene2functional(load_gene2highNOG(),
                                    'highNOGS/highNOG.feather',
                                    'highNOGS/highNOGS.txt')

_cache = {}
def load_functional_table(stored):
    import pandas as pd
    if stored not in _cache:
        table = pd.read_feather(stored[0])
        table.set_index('index', inplace=True)
        table = table.squeeze()

        names = [line.strip() for line in open(stored[1])]
        _cache[stored] = (table, names)
    return _cache[stored]


@TaskGenerator
def functional_profile(fname, functions):
    import pandas as pd
    print(f'functional({fname})')
    fname = fname.replace('.txt', '.feather')
    f = pd.read_feather(fname, nthreads=4)
    f.set_index('index', inplace=True)
    ftotal = f.sum()

    for odir,stored in functions.items():
        table, names = load_functional_table(stored)
        func = f.groupby(table).sum()
        func.index = [names[int(i)] for i in func.index]
        unch = ftotal - func.sum()
        unch.name = '-uncharacterized-'
        func = func.append(unch)
        func.sort_index(inplace=True)
        func.reset_index(inplace=True)
        func.to_feather(path.join(odir, path.basename(fname)))
    return fname


def add_up(dense):
    import pandas as pd
    single = {}
    for ix in dense.index:
        for base in ix.split(','):
            base = base.strip()
            if base in single:
                single[base] += dense.loc[ix].copy()
            else:
                single[base] = dense.loc[ix].copy()
    single = pd.DataFrame(single)
    return single.T

@TaskGenerator
def to_dense(files, funcdir):
    import pandas as pd
    dense = {}
    denseu = {}
    for i,f in enumerate(files):
        sample = f.split('/')[1].split('.')[0]
        f = f'{funcdir}/{sample}.feather'
        f = pd.read_feather(f, nthreads=8)
        f.set_index('index', inplace=True)
        dense[sample] = f['scaled']
        denseu[sample] = f['unique_scaled']
        if i % 100 == 99:
            print("to_dense({}) {}/{}".format(funcdir, i + 1, len(files)))
    dense = pd.DataFrame(dense)
    dense.fillna(0, inplace=True)
    dense = add_up(dense)
    dense.to_csv(f'{funcdir}/dense.txt', sep='\t')
    dense.reset_index(inplace=True)
    dense.to_feather(f'{funcdir}/dense.feather')

    denseu = pd.DataFrame(denseu)
    denseu.fillna(0, inplace=True)
    denseu = add_up(denseu)
    denseu.to_csv(f'{funcdir}/dense.unique.txt', sep='\t')
    denseu.reset_index(inplace=True)
    denseu.to_feather(f'{funcdir}/dense.unique.feather')
    return f'{funcdir}/dense.unique.feather'

@TaskGenerator
def rename_table(funcdir, _):
    import shutil
    shutil.copy2(f'{funcdir}/dense.txt', f'tables/{funcdir}.txt')
    shutil.copy2(f'{funcdir}/dense.feather', f'tables/{funcdir}.feather')
    shutil.copy2(f'{funcdir}/dense.unique.txt', f'tables/{funcdir}.unique.txt')
    shutil.copy2(f'{funcdir}/dense.unique.feather', f'tables/{funcdir}.unique.feather')
    return f'tables/{funcdir}.unique.feather'

samfiles = CachedFunction(list_samfiles)
samfiles.sort()


functions = {
        'bactNOGS': prepare_gene2bactNOG(),
        'kos': prepare_gene2ko(),
        'biggs': prepare_gene2bigg(),
        'highNOGS': prepare_gene2highNOG(),
        }


counts = []
functionals = []
for sample_ix,sf in enumerate(samfiles):
    c = count1(sf)
    counts.append(c)
    sc = scale_combine(c)
    f = functional_profile(sc, functions)
    functionals.append(f)

dense = []
for f in functions.keys():
    rename_table(f, to_dense(functionals, f))


save_totals(compute_totals(counts))
