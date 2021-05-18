import gzip
from jug import TaskGenerator, CachedFunction
from glob import glob
from jug.hooks import exit_checks
from jug.utils import identity

exit_checks.exit_after_time(hours=8)


_frare = None

@TaskGenerator
def load_full_rare():
    import pandas as pd
    rare = set(line.strip() for line in open('cold/rare-genes.old-names.txt'))
    orare = set()
    for ch in pd.read_table(
            'cold/GMGC.relationships.txt', chunksize=20_000_000, usecols=[0,2], header=None):
        orare.update(ch[ch[2].map(rare.__contains__)][0])
        print(len(orare))
    rare.update(orare)
    return rare

def paired(xs):
    prev = xs[0]
    for n in xs[1:]:
        yield (prev, n)
        prev = n


def tripled(xs):
    if len(xs) > 2:
        p0 = xs[0]
        p1 = xs[1]
        for n in xs[2:]:
            yield (p0, p1, n)
            p0, p1 = p1, n

def parse_mgm(fname):
    from itertools import groupby
    def _parse_mgm(fname):
        sample = fname.split('/')[-1].split('-')[0]
        for line in gzip.open(fname, 'rt'):
            if line[0] == '>':
                gene, contig = line.split('\t')
                contig = contig.split()[0][1:]
                gname, _, _, strand, _, _ = gene.split('|')
                gname = gname.split('_')[1]
                yield (sample+'_'+gname, contig, strand)
    for val,_ in groupby(_parse_mgm(fname)):
        yield val

@TaskGenerator
def count_from_file(fname, frare):
    from collections import defaultdict
    from itertools import groupby
    counts = defaultdict(int)
    for _,gs in groupby(parse_mgm(fname), key=lambda tup: tup[1]):
        gs = list(gs)
        for (g, _, _) in gs:
            counts['total', g in frare] += 1
        if len(gs) == 1:
            [(g, _, _)] = gs
            counts['singleton', g in frare] += 1
        else:
            for (g0, _, s0), (g1, _, s1) in paired(gs):
                counts['paired', g0 in frare, g1 in frare, s0 == s1] += 1
            for (g0, _, s0), (g1, _, s1), (g2, _, s2) in tripled(gs):
                counts['tripled', g0 in frare, g1 in frare, g2 in frare, s0 == s1, s1 == s2] += 1
    print(f'Finished {fname}')
    return counts

@TaskGenerator
def summarize(r):
    import pandas as pd
    nrare = r['paired', False, False, True] + \
            r['paired', False, False, False]

    rare = r['paired', True, True, True] + \
           r['paired', True, True, False]

    mixed = r['paired', False, True, True] + \
            r['paired', False, True, False] + \
            r['paired', True, False, True] + \
            r['paired', True, False, False]

    if rare == 0:
        rare = float('NaN')
    if mixed == 0:
        mixed = float('NaN')
    if nrare == 0:
        nrare = float('NaN')
    return pd.Series({
            'rare_fraction' : r['paired', True, True, True]/rare,
            'non_rare_fraction': r['paired', False, False, True]/nrare,
            'mixed_fraction': (r['paired', False, True, True]+ r['paired', True, False, True])/mixed,
            })


@TaskGenerator
def rename_all(raws):
    import pandas as pd
    rename = {
        ('total', False): 'total_non_rare',
        ('total', True): 'total_rare',
        ('singleton', False): 'singleton_non_rare',
        ('singleton', True): 'singleton_rare',
        ('paired', False, False, False): 'paired_nn_x',
        ('paired', False, False, True): 'paired_nn_m',
        ('paired', False, True, False): 'paired_nr_x',
        ('paired', False, True, True): 'paired_nr_m',
        ('paired', True, False, False): 'paired_rn_x',
        ('paired', True, False, True): 'paired_rn_m',
        ('paired', True, True, False): 'paired_rr_x',
        ('paired', True, True, True): 'paired_rr_m',
        ('tripled', False, False, False, False, False): 'tripled_nnn_xx',
        ('tripled', False, False, False, False, True): 'tripled_nnn_xm',
        ('tripled', False, False, False, True, False): 'tripled_nnn_mx',
        ('tripled', False, False, False, True, True): 'tripled_nnn_mm',
        ('tripled', False, False, True, False, False): 'tripled_nnr_xx',
        ('tripled', False, False, True, False, True): 'tripled_nnr_xm',
        ('tripled', False, False, True, True, False): 'tripled_nnr_mx',
        ('tripled', False, False, True, True, True): 'tripled_nnr_mm',
        ('tripled', False, True, False, False, False): 'tripled_nrn_xx',
        ('tripled', False, True, False, False, True): 'tripled_nrn_xm',
        ('tripled', False, True, False, True, False): 'tripled_nrn_mx',
        ('tripled', False, True, False, True, True): 'tripled_nrn_mm',
        ('tripled', False, True, True, False, False): 'tripled_nrr_xx',
        ('tripled', False, True, True, False, True): 'tripled_nrr_xm',
        ('tripled', False, True, True, True, False): 'tripled_nrr_mx',
        ('tripled', False, True, True, True, True): 'tripled_nrr_mm',
        ('tripled', True, False, False, False, False): 'tripled_rnn_xx',
        ('tripled', True, False, False, False, True): 'tripled_rnn_xm',
        ('tripled', True, False, False, True, False): 'tripled_rnn_mx',
        ('tripled', True, False, False, True, True): 'tripled_rnn_mm',
        ('tripled', True, False, True, False, False): 'tripled_rnr_xx',
        ('tripled', True, False, True, False, True): 'tripled_rnr_xm',
        ('tripled', True, False, True, True, False): 'tripled_rnr_mx',
        ('tripled', True, False, True, True, True): 'tripled_rnr_mm',
        ('tripled', True, True, False, False, False): 'tripled_rrn_xx',
        ('tripled', True, True, False, False, True): 'tripled_rrn_xm',
        ('tripled', True, True, False, True, False): 'tripled_rrn_mx',
        ('tripled', True, True, False, True, True): 'tripled_rrn_mm',
        ('tripled', True, True, True, False, False): 'tripled_rrr_xx',
        ('tripled', True, True, True, False, True): 'tripled_rrr_xm',
        ('tripled', True, True, True, True, False): 'tripled_rrr_mx',
        ('tripled', True, True, True, True, True): 'tripled_rrr_mm',
    }
    return pd.DataFrame({
        s:pd.Series({rename[k]:v for k,v in r.items()})
            for s,r in raws.items()})

@TaskGenerator
def write_renamed(renamed):
    renamed.to_csv('tables/rare-organization.tsv', sep='\t')
mgms = CachedFunction(glob, '/g/bork1/coelho/DD_DeCaF/genecats/sources/assemble-genes/orfs/*.gz')
mgms.sort()


frare = load_full_rare()
final = {}
raws = {}
for f in mgms:
    sample = f.split('/')[-1].split('-')[0]
    r = count_from_file(f, frare)
    raws[sample] = r
    final[sample] = summarize(r)
final = identity(final)
raws = identity(raws)
renamed = rename_all(raws)
write_renamed(renamed)
