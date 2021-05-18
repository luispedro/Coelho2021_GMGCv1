from shutil import copyfile
from glob import glob
import pandas as pd

qc = pd.read_table('cold/GMBC10.meta.tsv', index_col=0)
binfiles = glob('/g/scb2/bork/orakov/GMGC/Projects/bins/*/*.fa.gz')
binfiles = [b for b in binfiles if '.unbinned' not in b]

rename = qc[['genome']].reset_index().set_index('genome').squeeze().to_dict()
for bf in binfiles:
    ofile = rename[bf.split('/')[-1][:-len('.fa.gz')]]
    copyfile(bf, 'cold/genome.bins/'+ofile + '.fna.gz')
    
    
