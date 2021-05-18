import numpy as np
import pandas as pd
from jug import TaskGenerator
from glob import glob

SAMPLES_DIR = 'samples_20180516/'
_c90d = None

def load_c90d():
    global _c90d
    if _c90d is None:
        import pickle
        r_rename = pickle.load(open('cold/r_rename.pkl', 'rb'))
        c90 = pd.read_table('cold/GMGC.95nr.90lc_cluster.tsv', header=None, index_col=1, squeeze=True)
        c90.index = c90.index.map(r_rename.get)
        _c90d = c90.to_dict()
    return _c90d

_gfd = None
def load_gfd():
    import pandas as pd
    global _gfd
    if _gfd is None:
        import pickle

        print(0)
        gf = pd.read_table('cold/GMGC.gf.cluster_members.tsv', header=None, index_col=1, squeeze=True)
        print(1)
        r_rename = pickle.load(open('cold/r_rename.pkl', 'rb'))
        print(2)
        print(3)
        print(4)
        gf.index = gf.index.map(r_rename.get)
        print(5)
        _gfd = gf.to_dict()
        print(6)
    return _gfd



@TaskGenerator
def rarefify_project(perm_fname, projection):
    if projection == '90':
        proj = load_c90d()
    elif projection == 'gf':
        proj = load_gfd()
    else:
        raise ValueError("Unknwon projection: {}".format(projection))
    seen = set()
    curve = [0]
    for p in open(perm_fname):
        p = p .strip()
        for line in open(SAMPLES_DIR + p):
            line = line.strip()
            gf = proj.get(line)
            if gf: seen.add(gf)
        curve.append(len(seen))
    return np.array(curve)
ps90 = []
psf = []
for p in sorted(glob('outputs/rarefaction_all/perm_*')):
    ps90.append(rarefify_project(p, '90'))
    psf.append(rarefify_project(p, 'gf'))
