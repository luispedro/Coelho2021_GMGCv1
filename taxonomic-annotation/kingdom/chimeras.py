import numpy as np
from jug import TaskGenerator
import pandas as pd
from jugfile import diamonds




def slow_chimera_candidate(po):
    for i in range(len(po)):
        for j in range(i+1, len(po)):
            h0 = po.iloc[i]
            h1 = po.iloc[j]
            if h0.qstart > h1.qstart:
                h0,h1 = h1,h0
            if h1.qstart > h0.qend or h0.qend - h1.qstart < 10:
                return True
def chimera_candidate(po):
    return np.subtract.outer(po.qend, po.qstart).min() < 10

fname = "output/GMGC.95nr.m8.split.103.m8"
@TaskGenerator
def count_chimera_candidates(fname):
    blast = pd.read_table(fname, header=None, names=['q', 'tr', 'id', 'len', 'mis', 'gaps', 'qstart', 'qend', 'hstart', 'hend', 'evalue', 'bit'])
    blast = blast.loc[blast.q.map(lambda g: not g.startswith('Fr12_'))]                           
    blast = blast[blast.id > 70]
    gs = blast.groupby(blast.q).groups
    pos = blast[['qstart', 'qend']]
        
    candidates = []
    tested = 0
    for v in gs.values():
        if len(v) < 2: continue
        tested += 1
        if chimera_candidate(pos.loc[v]):
            assert slow_chimera_candidate(blast.loc[v])
            [g] = set(blast.q[v])
            candidates.append(g)
        if (tested %1_000) == 0:
            print("Tested {}k ({} candidates)".format(tested/1_000, len(candidates)))
    return tested, candidates

candidates = []


for fname in diamonds:
    candidates.append( count_chimera_candidates(fname))
