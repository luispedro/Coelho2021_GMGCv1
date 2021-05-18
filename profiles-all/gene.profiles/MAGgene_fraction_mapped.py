from glob import glob
import pandas as pd
from jug import TaskGenerator, CachedFunction


UNIQUES = CachedFunction(glob, 'outputs.txt/*.unique.txt.gz')
UNIQUES.sort()

SAMPLES = [u.split('/')[1].split('.')[0] for u in UNIQUES]


# samples = set(line.strip() for line in open('../samples.txt'))
# SAMPLES = [s for s in SAMPLES if s in samples]


magged = None
@TaskGenerator
def fraction_mag_mapped(sample):
    global magged
    if magged is None:
        magged = set()
        for line in open('cold/MAGgenes.txt'):
            magged.add(line.strip())

    Su = pd.read_table('outputs.txt/{}.unique.txt.gz'.format(sample) ,  index_col=0, squeeze=True)
    S = pd.read_table('outputs.txt/{}.txt.gz'.format(sample) ,  index_col=0, squeeze=True)
    fU = Su.index.map(magged.__contains__).values.mean()
    f = S.index.map(magged.__contains__).values.mean()
    return pd.Series({'all': f, 'unique_only': fU})

@TaskGenerator
def save_detections(detections):
    pd.DataFrame(detections).T.to_csv('MAGgenes.detections.tsv', sep='\t')

mapping = {}
for s in SAMPLES:
    mapping[s] = fraction_mag_mapped(s)
save_detections(mapping)
