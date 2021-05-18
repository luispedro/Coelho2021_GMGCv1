from jug import TaskGenerator, bvalue
from jug.hooks import exit_checks
exit_checks.exit_if_file_exists('jug.exit')

M6_NAMES = 'qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore'.split()

def use_name(na):
    import hashlib
    return hashlib.sha256(na.encode('ascii')).hexdigest()[:3] == 'fff'

@TaskGenerator
def basic_stats(fname):
    import pandas as pd
    t = pd.read_table(fname, header=None, names=M6_NAMES)
    res = {
            'total_qs': len(set(t.qseqid)),
    }
    no_self_hits = t.query('qseqid != sseqid')
    allqs = set(no_self_hits.qseqid)
    res['any_hit'] = len(allqs)
    res['use_name'] = sum(use_name(q) for q in allqs)
    no_self_hits = no_self_hits.query('pident >= 90')
    res['any_hit_90'] = len(set(no_self_hits.qseqid))
    no_self_hits = no_self_hits.query('pident >= 92')
    res['any_hit_92'] = len(set(no_self_hits.qseqid))
    no_self_hits = no_self_hits.query('pident >= 95')
    res['any_hit_95'] = len(set(no_self_hits.qseqid))
    return pd.Series(res)


def iter_fasta(fname):
    header = None
    seq = []
    for line in open(fname):
        line = line.strip()
        if line[0] == '>':
            if header is not None:
                if len(seq) == 1:
                    [seq] = seq
                else:
                    seq = ''.join([s for s in seq])
                yield (header, seq)
            header = line
            seq = []
        else:
            seq.append(line)

@TaskGenerator
def split_fasta(fname, base, split_size_in_mb):
    from os import path
    splits = []
    next_i = 0
    split_size = split_size_in_mb * 1000 * 1000
    cur = split_size + 1
    for h,seq in iter_fasta(fname):
        if len(seq) + cur > split_size:
            oname = path.join(base, 'split.{}.fa'.format(next_i))
            splits.append(oname)
            out = open(oname, 'wt')
            next_i += 1
            cur = 0
        cur += len(seq)
        out.write('{}\n{}\n'.format(h, seq))
    return splits

@TaskGenerator
def run_diamond(s):
    oname = s.replace('.fa', '.m6')
    from jug.utils import jug_execute
    jug_execute.f([
        '/g/scb2/bork/mocat/software/diamond/0.9.24/diamond',
        'blastp',
        '--threads', '12',
        '-d', 'GMGC.95nr.faa',
        '-q', s,
        '--outfmt', '6',
        '-o', oname])
    return oname

@TaskGenerator
def post_process1(m6):
    import skbio.alignment
    from skbio.sequence import DNA,Protein
    import pandas as pd
    import numpy as np
    from fasta_reader import IndexedFastaReader
    ix = IndexedFastaReader('GMGC.95nr.fna', use_mmap=True)
    cache_ix = {}
    def get_ix(n):
        if n not in cache_ix:
            if len(cache_ix) > 1000_000:
                cache_ix.clear()
            cache_ix[n] = ix.get(n).decode('ascii') \
                                .replace('N','A') \
                                .replace('K','A') \
                                .replace('S','A') \
                                .replace('M','A') \
                                .replace('W','A') \
                                .replace('D','A') \
                                .replace('R','A') \
                                .replace('Y','A')
        return cache_ix[n]

    t = pd.read_table(m6, header=None, names=M6_NAMES)
    print("Loaded table...: ({} entries)".format(len(t)))

    no_self_hits = t.query('qseqid != sseqid').query('pident >= 90')
    print("Filtered table... ({} entries)".format(len(no_self_hits)))

    no_self_hits = no_self_hits[no_self_hits.qseqid.map(use_name)]
    print("Re-filtered table... ({} entries)".format(len(no_self_hits)))

    data = []
    n = 0
    for _,row in no_self_hits.iterrows():
        na = row['qseqid']
        #if not use_name(na):
        #    continue
        #print(na)
        nb = row['sseqid']
        sa = get_ix(na)
        sb = get_ix(nb)
        sw_dna = skbio.alignment.local_pairwise_align_ssw(DNA(sa),DNA(sb))
        [(sa_start, sa_end), (sb_start, sb_end)] = sw_dna[2]
        a = sw_dna[0]
        al_id = np.mean(a.conservation() == 1.0)
        data.append((na, nb, al_id, sa_start, sa_end, sb_start, sb_end, len(sa), len(sb)))
        #print(data[-1])
        n += 1
        if n % 10 == 0:
            print(n, len(no_self_hits), '{:.1%}'.format(n/len(no_self_hits)), m6)

    data = pd.DataFrame(data, columns=['name_a', 'name_b', 'align_id', 'a_start', 'a_end', 'b_start', 'b_end', 'a_len', 'b_len'])
    oname = m6.replace('.m6','.tsv')
    data.to_csv(oname, sep='\t')
    return oname

def cover_full(row):
     return (row['a_start'] == 0 and row['a_end']+1 == row['a_len']) \
             or (row['b_start'] == 0 and row['b_end']+1 == row['b_len'])

def ends_overlap(row):
    [a_start, a_end, b_start, b_end, a_len, b_len] = row[['a_start', 'a_end', 'b_start', 'b_end', 'a_len', 'b_len']]
    MARGIN = 5
    return ((a_len - a_end - 1 <= MARGIN) and (b_start <= MARGIN)) or ((b_len - b_end - 1 <= MARGIN) and (a_start <= MARGIN))

@TaskGenerator
def get_overlapped(t):
    import pandas as pd
    final = pd.read_table(t)
    f95 = final.query('align_id >= 0.95')
    maybe = set()
    for _,row in f95.iterrows():
        if ends_overlap(row) and not cover_full(row):
            maybe.add(row['name_a'])
    maybe = list(maybe)
    maybe.sort()
    return maybe


@TaskGenerator
def used_genes():
    used = []
    for line in open('cold/derived/GMGC10.headers'):
        if use_name(line.strip()):
            used.append(line.strip())
    return used


@TaskGenerator
def save_results(used, overlapped):
    import pandas as pd
    maybe = set()
    for vs in overlapped.values():
        maybe.update(vs)

    has_overlaps = pd.Series([u in maybe for u in used],index=used)
    pd.DataFrame({'has_overlaps' : has_overlaps}).to_csv('tables/has_overlaps.tsv', sep='\t')

splits = split_fasta('GMGC.95nr.faa', 'GMGC95.splits', 400)
splits = bvalue(splits)

partials = []
overlapped = {}
for s in splits[::-1]:
    partials.append(run_diamond(s))
    p = post_process1(partials[-1])
    overlapped[s] = get_overlapped(p)

post = {p:basic_stats(p) for p in partials}

used = used_genes()
save_results(used, overlapped)
