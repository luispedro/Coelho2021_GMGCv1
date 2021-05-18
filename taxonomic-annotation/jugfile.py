from glob import glob
from jug import TaskGenerator, barrier, Tasklet
DIAMOND = '/g/bork3/home/coelho/work/MOCAT/public/bin/diamond'
UNIREF_DB = 'uniref100.wtaxid'
NR_DIAMOND_CPUS = 32
INPUT_FNA = 'Pig2016.fna'
INPUT_BASE = 'Pig2016'

def mkdir_p(dirname):
    from os import makedirs
    try:
        makedirs(dirname)
    except:
        pass

partialdir = 'partials.{}/'.format(INPUT_BASE)
splits1 = '{}/splits'.format(partialdir)
splits2 = '{}/splits2'.format(partialdir)
mkdir_p(splits1)
mkdir_p(splits2)

@TaskGenerator
def jug_execute(args, check_exit=True, return_token=False, run_after=None):
    import subprocess
    ret = subprocess.call(args)
    if check_exit and ret != 0:
        raise SystemError("Error in system")
    if return_token:
        from jug.hash import hash_one
        return hash_one(('jug_execute', 'output_hash', args))

@TaskGenerator
def compute_nr_lines(ifile, token):
    n = 0
    for i,line in enumerate(open(ifile)):
        if i % 2 == 0 and line[0] != '>':
            raise ValueError("Not a single line FASTA")
        n += 1
    n //= 199
    if n % 2 != 0:
        n += 1
    return n

@TaskGenerator
def parse_B1(ofile, tokens):
    with open(ofile, 'w') as output:
        seen = set()
        for ifile in sorted(glob('{}/*.daa.m8'.format(splits1))):
            for line in open(ifile):
                tokens = line.rstrip().split('\t')
                q = tokens[0]
                hit = tokens[1]
                start = tokens[8]
                end = tokens[9]
                evalue = tokens[10]
                bitscore = tokens[11]
                if q not in seen:
                    output.write("{q}\t{hit}\t{start}\t{end}\t{evalue}\n".format(q=q, hit=hit, start=start, end=end, evalue=evalue))
                    seen.add(q)
@TaskGenerator
def prepare_B2(ifile, ofile):
    from Bio.SeqIO import FastaIO
    from collections import defaultdict

    interesting = defaultdict(list)
    for line in open(ifile):
        tokens = line.rstrip().split('\t')
        interesting[tokens[1]].append(tokens)
    interesting = dict(interesting)
    with open(ofile, 'w') as output:
        for seq in FastaIO.FastaIterator(open('./uniref100.wtaxid.faa')):
            for (n, _, st, e, evalue) in interesting.get(seq.id, []):
                output.write(">{}_{}_{} {} {}\n".format(seq.id, st, e, n, evalue))
                s = seq.seq[int(st)-1:int(e)]
                output.write(str(s))
                output.write('\n')
@TaskGenerator
def generate_taxonomic_map(ifile, m8s, faa, ofile):
    from collections import defaultdict
    import lca
    from sys import stderr

    interesting = defaultdict(list)
    for line in open(ifile):
        tokens = line.rstrip().split('\t')
        name = "{}_{}_{}".format(tokens[1], tokens[2], tokens[3])
        interesting[name].append((tokens[0], float(tokens[4])))

    neighborhood = defaultdict(list)
    for sp in m8s:
        for line in open(sp):
            tokens = line.rstrip().split('\t')
            evalue = float(tokens[10])
            for gname,thresh in interesting[tokens[0]]:
                if evalue <= thresh:
                    neighborhood[gname].append(tokens[1])
    for line in open(faa):
        if line[0] == '>':
            gene = line.split()[0]
            gene = gene[1:]
            if gene not in neighborhood:
                neighborhood[gene] = []

    needtaxa = set()
    for v in neighborhood.values(): needtaxa.update(v)

    taxa = {}
    for line in open('uniref100.taxid'):
        gene,taxaid = line.rstrip().split('\t')
        if gene in needtaxa:
            taxa[gene] = taxaid
    ancestors, ranks = lca.load_ncbi_tree()

    names = lca.load_ncbi_names()

    with open(ofile, 'w') as output:
        output.write("{}\t{}\t{}\t{}\n".format('gene', 'NCBI TaxID', 'Rank', 'Name'))
        merged = {
                # This is from a mismatch between NCBI and uniref versions
                # Equivalent taxids were found manually (and verified).
                '1094619': '67593',
                '1283334': '209052',
                }
        for line in open('taxdump/merged.dmp'):
            tokens = line.split('\t')
            merged[tokens[0]] = tokens[2]
        not_found = 0
        has_not_found = 0
        for gene,ns in neighborhood.items():
            cur_has_not_found = False
            for n in ns:
                if n not in taxa:
                    not_found +=1
                    cur_has_not_found = True
            has_not_found += cur_has_not_found
            ns = [taxa[n] for n in ns if n in taxa]
            ns = [merged.get(n, n) for n in ns]
            ps = [lca.path(ancestors, n) for n in ns]
            if ps:
                final = lca.lca(ps)
                finalid = final[-1]
                rank = 'no rank'
                for i in final[::-1]:
                    if rank == 'no rank':
                        rank = ranks[i] # Strain is not a rank on NCBI
                output.write("{}\t{}\t{}\t{}\n".format(gene, finalid, rank, names[finalid]))
            else:
                output.write("{}\t1\tno rank\tunclassified\n".format(gene))
        stderr.write("not found: {} (in {} genes)\n".format(not_found, has_not_found))

start = jug_execute(['bash', '-c', "/g/bork5/coelho/oceanblast/src/convert < {} > {}.faa".format(INPUT_FNA, INPUT_BASE)], return_token=True)
nr_lines = compute_nr_lines('{}.faa'.format(INPUT_BASE), start)
jug_execute(['split', '-d', '-l', Tasklet(nr_lines, str), '-a', '4', INPUT_BASE + '.faa', '{}/{}.split'.format(splits1, INPUT_BASE)])

barrier()

splits = [sp for sp in glob('{}/{}*split*'.format(splits1, INPUT_BASE)) if 'daa' not in sp]
splits.sort() # heuristic as often the lower valued splits have longer genes

tokens = []
for sp in splits:
    t = jug_execute([DIAMOND, 'blastp', '-v', '--db', 'uniref100.wtaxid', '--query', sp, '-p', str(NR_DIAMOND_CPUS), '-a', sp + '.daa'], return_token=True)
    jug_execute([DIAMOND, 'view' ,'-a', sp + '.daa', '-o', sp + '.daa.m8'], run_after=t)
    tokens.append(t)
parse_B1('{}/hits-1.top'.format(partialdir), tokens)
barrier()
prepare_B2('{}/hits-1.top'.format(partialdir), '{}/B2.faa'.format(partialdir))
barrier()
jug_execute(['split', '-d', '-l', Tasklet(nr_lines, str), '-a', '4', '{}/B2.faa'.format(partialdir), '{}/{}.B2.split'.format(splits2, INPUT_BASE)])
barrier()

splits2 = [sp for sp in glob('{}/{}.B2.split*'.format(splits2, INPUT_BASE)) if 'daa' not in sp]
splits2.sort()
m8s = []
for sp in splits2:
    m8 = sp + '.daa.m8'
    j = jug_execute(
                [DIAMOND, 'blastp', '-v', '--db', 'uniref100.wtaxid', '--query', sp, '-p', str(NR_DIAMOND_CPUS), '-a', sp + '.daa'],
                return_token=True
                )
    jug_execute([DIAMOND, 'view' ,'-a', sp + '.daa', '-o', m8], run_after=j)
    m8s.append(m8)

barrier()

generate_taxonomic_map("{}/hits-1.top".format(partialdir), m8s, '{}.faa'.format(INPUT_BASE), '{}.taxonomic.map'.format(INPUT_BASE))
