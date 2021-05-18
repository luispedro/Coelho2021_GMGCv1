from os import path
from itertools import chain
from fasta_reader import IndexedFastaReader
import skbio.alignment
import skbio.sequence
import skbio.io
from sys import argv
import subprocess

index_fname = argv[1]
query_fname = argv[2]

index_base, index_fname  = path.split(path.abspath(index_fname))

class GetNames(object):
    def __init__(self, fname):
        self.headers = open(fname, 'rb')
    def get(self, ix):
        self.headers.seek(32 * ix)
        return self.headers.readline().decode('ascii').strip()

Qs = {}
for fa in skbio.io.read(query_fname, format='fasta'):
    Qs[fa.metadata['id']] = str(fa)


headers = GetNames(f'{index_base}/kmer.index/{index_fname}.names.32')
print("Loaded headers")

index = IndexedFastaReader(path.join(index_base, index_fname))

data = subprocess.Popen(['Query', '-i', query_fname, '-o', '/dev/stdout', '-1', f'{index_base}/kmer.index/{index_fname}.kmer.ix1', '-2', f'{index_base}/kmer.index/{index_fname}.kmer.ix2'],
        stdout=subprocess.PIPE)

matches = []
for line in chain(data.stdout, [b'END']):
    if line.startswith(b'CmdArgs'): continue
    line = line.decode('ascii')
    if line[0] == '>' or line == 'END':
        if len(matches):
            matches.sort(key=lambda m: m[1]['optimal_alignment_score'], reverse=True)
            for fah, m in matches:
                print(f'{active}\t{fah}\t{m.optimal_alignment_score}')
            matches = []
        if line == "END":
            break
        active = line[1:].strip()
        fa = Qs[active]
        sw = skbio.alignment.StripedSmithWaterman(fa)
    else:
        ix = int(line.strip())
        name = headers.get(ix)
        matches.append((name, sw(index.get(name).decode('ascii'))))

