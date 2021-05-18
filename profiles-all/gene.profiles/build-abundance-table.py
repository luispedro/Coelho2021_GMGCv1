from jug import TaskGenerator, CachedFunction
from glob import glob
import pandas as pd
from jug.hooks.exit_checks import exit_if_file_exists, exit_env_vars
from jug.utils import identity

exit_env_vars()
exit_if_file_exists('jug.exit')

_rename = None
def lazy_load_rename():
    global _rename
    if _rename is None:
        import pickle
        _rename = pickle.load(open('cold/rename.pkl', 'rb'))
    print("Loaded rename")
    return _rename

_sizes = None
def lazy_load_sizes():
    global _sizes
    if _sizes is None:
        import pandas as pd
        _sizes = pd.read_table('tables/GMGC.95nr.sizes', index_col=0, squeeze=True)
    print("Loaded sizes")
    return _sizes

def scale(g, sizes):
    total = g.sum()
    gn = g / sizes[g.index]
    gn *= total /gn.sum()
    return gn

def sample_rename(s):
    import pandas as pd
    exdog2ena = pd.read_table('tables/ExDog2ENA.tsv', index_col=0, squeeze=True).to_dict()
    return exdog2ena[s]

@TaskGenerator
def sample_table(fu):
    sample = fu.split('/')[1].split('.')[0]
    print("Doing "+sample)
    if sample.startswith('ExDog'):
        sample = sample_rename(sample)
        print("Renamed to "+sample)

    sizes = lazy_load_sizes()
    rename = lazy_load_rename()
    f = fu.replace('.unique', '')

    non_unique = pd.read_table(f, index_col=0, squeeze=True)
    unique = pd.read_table(fu, index_col=0, squeeze=True)

    non_unique_scaled = scale(non_unique, sizes)
    non_unique_normed = 10_000_000 * non_unique_scaled/non_unique_scaled.sum()
    non_unique_normed = non_unique_normed.round()

    pasted = pd.DataFrame({'raw_unique': unique, 'raw': non_unique, 'scaled': non_unique_scaled, 'normed10m' : non_unique_normed})
    pasted.fillna(0, inplace=True)
    pasted.rename(index=rename, inplace=True)
    pasted.sort_index(inplace=True)
    pasted['sample'] = sample
    pasted = pasted.reindex(columns=['sample', 'scaled', 'raw', 'raw_unique', 'normed10m'], )

    oname = 'outputs.per.gene/{}.txt'.format(sample)
    pasted.to_csv(oname, sep='\t')
    print("Done "+oname)
    return oname

def copy_file(out, p, skipfirst):
    with open(p, 'rt') as ifile:
        if skipfirst:
            _ = ifile.readline()
        while True:
            block = ifile.read(8192)
            if not block:
                return
            out.write(block)

@TaskGenerator
def concat_table(partials):
    import safeout
    import pandas as pd
    import lzma
    import subprocess
    import io
    first = True

    with safeout.safeout('cold/profiles/gene-abundance.tsv.xz', 'wb') as r_out:
        with subprocess.Popen(['xz',
                            '--threads=24',
                            '--to-stdout',
                            '-',
                            ],
                            stdout=r_out.tfile,
                            stdin=subprocess.PIPE) as xz:
            with io.TextIOWrapper(xz.stdin, 'ascii') as out:
                for i,p in enumerate(partials):
                    copy_file(out, p, skipfirst=(not first))
                    print(i, p)
                    first = False
UNIQUES = [u for u in CachedFunction(glob, 'outputs.txt/*.*') if '.unique.' in u]
UNIQUES.sort()

partials = []
for fu in UNIQUES:
    partials.append(sample_table(fu))

concat_table(identity(partials))
