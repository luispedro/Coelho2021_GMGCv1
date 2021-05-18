from taxonomic import ncbi
import pandas as pd
fraction_rare = pd.read_table('tables/fraction-rare.tsv', index_col=0)
specIs = pd.read_table('/g/scb2/bork/mocat/freezer/prok-refdb/v12.0.0/combinedVsearch3.50p.20n.norm.opt.0.042.clustering.map', header=None, names=['specI', 'genome'])

n = ncbi.NCBI()

fr = fraction_rare.loc[specIs.query('specI == "specI_v3_Cluster95"').genome]
print("Using specI_v3_95")
print("Nr genomes: {}".format(len(fr)))
print("Mean: {}".format(fr.fraction_rare.mean()))

fr = fr.groupby(fr.index.map(lambda ix: ix.split('.')[0])).first()
print("Just 1 strain")
print("Nr genomes: {}".format(len(fr)))
print("Mean: {}".format(fr.fraction_rare.mean()))

fr = fraction_rare.loc[fraction_rare.index.map(lambda x: '562' in n.path(x.split('.')[0]))]
print("Using taxid <- 562")
print("Nr genomes: {}".format(len(fr)))
print("Mean: {}".format(fr.fraction_rare.mean()))

fr = fr.groupby(fr.index.map(lambda ix: ix.split('.')[0])).first()
print("Just 1 strain")
print("Nr genomes: {}".format(len(fr)))
print("Mean: {}".format(fr.fraction_rare.mean()))

