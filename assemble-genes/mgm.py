from os import path
from jug import TaskGenerator, barrier, CachedFunction, CompoundTaskGenerator
from jug.utils import jug_execute
from glob import glob
import subprocess


def name_gene(prefix, gname):
    gene,nr = gname.split('_')
    assert gene == 'gene'
    prefix, assembled = prefix.split('-')
    assert assembled == 'assembled'
    return prefix + '_' + assembled

MGM_DIR = '/g/bork3/home/coelho/work/MOCAT/public/ext/metagenemark'
def extract_table(ifile, ofile, prefix):
    n = 0
    header = ifile.readline()
    assert header.strip().split()[0] == 'Gene'
    header2 = ifile.readline()
    assert header2.strip()[0] == '#'

    while True:
        line = ifile.readline().strip()
        if not line:
            return n
        if line.startswith('--------'):
            line = ifile.readline()
            assert line.startswith('sequence is noncoding')
            return 0
        tokens = line.split()
        gene,strand,start,end = tokens[:4]
        complete = True
        if start[0] == '<':
            start = start[1:]
            complete = False
        if end[0] == '>':
            end = end[1:]
            complete = False
        ofile.write("\t".join([(prefix.replace('-assembled', '') + '_' + gene), strand, start, end, str(complete)]))
        ofile.write("\n")
        n += 1

def extract_seqs(n, ifile, ofile, prefix):
    if n == 0:
        return
    while True:
        line = ifile.readline()
        if not line:
            return
        if not line.strip():
            if n == 0:
                return
            continue
        if line[0] == '>':
            start,_ = line.split('|', maxsplit=1)
            line = start.replace('gene', prefix)
            line += '\n'
            n -= 1
        if ofile is not None:
            ofile.write(line)


def parse_mgm_output(mgm_in, mgm_out, prefix):
    print(f"parse_mgm_output({mgm_in}, {mgm_out}, {prefix})")
    n = 0
    with open(mgm_in) as ifile, \
            open(mgm_out + '.fna', 'wt') as ofile_fna, \
            open(mgm_out + '.tab', 'wt') as ofile_tab:
        section = 'header'
        while True:
            line = ifile.readline()
            if not line:
                return n
            line = line.strip()
            if not line:
                continue

            if line.startswith('Predicted genes'):
                section_size = extract_table(ifile, ofile_tab, prefix)
                n += section_size
            elif line.startswith('Predicted proteins:'):
                extract_seqs(section_size, ifile, None, prefix)
            elif line.startswith('Nucleotide sequence of predicted genes:'):
                extract_seqs(section_size, ifile, ofile_fna, prefix.replace('-assembled', ''))


mgm_out = 'orfs'

@TaskGenerator
def run_mgm(fna, mgm_out, prefix):
    import tempfile
    from jug.utils import sync_move
    from shutil import move
    with tempfile.NamedTemporaryFile(prefix='mgm.out', delete=False) as tfile:
        tfile.close()
        print("Calling MGM")
        jug_execute.f([f'{MGM_DIR}/gmhmmp', '-a', '-d', '-f', 'L',  '-m', f'{MGM_DIR}/MetaGeneMark_v1.mod', '-o', tfile.name, fna])
        print("Parsing...")
        parse_mgm_output(tfile.name, mgm_out, prefix)
        print("Gzipping...")
        jug_execute.f(['gzip', tfile.name])
        print("Moving to final destination: " + mgm_out + '.gz')
        move(tfile.name + '.gz', mgm_out + '.gz.tmp')
        sync_move(mgm_out + '.gz.tmp', mgm_out + '.gz')

for sample in open('samples.txt'):
    sample = sample.strip()
    fna = f'assembled/{sample}-assembled.fna'
    prefix = path.basename(fna)[:-len('.fna')]
    mgm_out = f'orfs/{prefix}.mgm'
    run_mgm(fna, mgm_out, prefix)

