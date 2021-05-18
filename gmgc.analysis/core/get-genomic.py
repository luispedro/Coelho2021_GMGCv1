from jug import TaskGenerator, Tasklet
from jug.utils import jug_execute

@TaskGenerator
def get_genomic_occurrences():
    from safeout import safeout
    BASEDIR = '/g/bork1/huerta/resource_freezer/prok-refdb/v12.0.0/eggnog_annotations'
    oname = 'outputs/genomic-occurrences.txt'
    with safeout(oname, 'wt') as output:
        for line in open(f'{BASEDIR}/prok-refdb-v12.0.0.non_env.nr.emapper_annotations-v1.tsv'):
            tokens = line.rstrip('\n').split('\t')
            bactNOG = [b for b in tokens[10].split(',') if 'bactNOG' in b]
            if len(bactNOG):
                [bactNOG] = bactNOG
                ts = set()
                for gid in tokens[1].split(','):
                    taxid = gid.split('.')[0]
                    ts.add(taxid)
                for t in ts:
                    output.write(f'{bactNOG}\t{t}\n')
    return oname

@TaskGenerator
def compute_relative(_):
    import pandas as pd
    gocc = pd.read_table('outputs/genomic-occurrences.txt.sorted', header=None, names=['bactNOG', 'TaxID'])
    ntaxids = len(set(gocc['TaxID'].values))
    presence = gocc.groupby(gocc.bactNOG).count().squeeze()
    rel = presence/ntaxids
    oname = 'outputs/genomic.relative.txt'
    pd.DataFrame({'relative': rel}).to_csv(oname, sep='\t')
    return oname

gocc = get_genomic_occurrences()
def add_sort(c):
    return c + '.sorted'
t = jug_execute(['sort', '-S8G', '-u', gocc, '-o', Tasklet(gocc, add_sort)])
compute_relative(t)

