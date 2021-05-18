from glob import glob
import pandas as pd
from jug import TaskGenerator, CachedFunction
from jug import mapreduce


UNIQUES = CachedFunction(glob, 'outputs.txt/*.unique.txt.gz')
UNIQUES.sort()

SAMPLES = [u.split('/')[1].split('.')[0] for u in UNIQUES]

def load_group(name):
    if name == 'rare_genes':
        return set(line.strip() for line in open('tables/rare-genes.txt'))
    if name == 'complete_genes':
        return set(line.strip() for line in open('cold/derived/GMGC10.complete.original_names.txt'))

_cache = {}
def fraction_in_class(sample, group):
    if group not in _cache:
        _cache[group] = load_group(group)
    active = _cache[group]

    S = pd.read_table('outputs.txt/{}.txt.gz'.format(sample) ,  index_col=0, squeeze=True)
    Ssum = S.groupby(active.__contains__).sum().to_dict()
    Smedian = S.groupby(active.__contains__).median().to_dict()
    Scount = S.groupby(active.__contains__).count().to_dict()
    return pd.Series({
            'active_count': Scount.get(True, 0),
            'active_sum': Ssum.get(True, 0),
            'active_median': Smedian.get(True, 0),
            'inactive_count': Scount.get(False, 0),
            'inactive_sum': Ssum.get(False, 0),
            'inactive_median': Smedian.get(False, 0),
            })

@TaskGenerator
def rare_rount(sample):
    return fraction_in_class(sample, 'rare_genes')

@TaskGenerator
def complete_count(sample):
    return fraction_in_class(sample, 'complete_genes')

@TaskGenerator
def save_table(counts, oname):
    data = pd.DataFrame(counts, index=SAMPLES)
    data['total_count'] = data.eval('active_count + inactive_count') 
    data['total_sum'] = data.eval('active_sum + inactive_sum') 
    data.to_csv(oname, sep='\t')

#counts = mapreduce.map(rare_counts, SAMPLES, map_step=32)
#save_table(counts,'tables/rare-gene-fraction-mapped.tsv', sep='\t')

counts = mapreduce.map(complete_count, SAMPLES, map_step=32)
save_table(counts,'tables/complete-gene-fraction-mapped.tsv')
