import pandas as pd
from taxonomic import ncbi
n = ncbi.NCBI()
taxonomic = pd.read_table('/g/bork1/coelho/DD_DeCaF/genecats.cold/GMGC10.taxonomic.map', index_col=0, engine='c')
species = pd.read_table('/g/bork1/coelho/DD_DeCaF/genecats.cold/GMGC10.species.match.map',  header=None, usecols=[1,2], index_col=0, squeeze=True, names=['gene', 'TaxID'], engine='c')
superkingdom = pd.read_table('/g/bork1/coelho/DD_DeCaF/genecats.cold/GMGC10.kingdom.annotation', header=None, names=['gene', 'superkingdom'], index_col=0, squeeze=True, engine='c')
taxid = taxonomic['NCBI TaxID'].to_dict()
d_superkingdom = superkingdom.to_dict()
d_species = species.to_dict()
d_predicted_taxid = taxonomic['NCBI TaxID'].to_dict()
taxid = taxonomic['NCBI TaxID'][taxonomic.Rank == 'species'].to_dict()

gs = {}
for g,t in taxid.items():
    gs[g] = n.ancestors.get(str(t), '1')
    if len(gs) % 10_000_000 == 0:
        print(len(gs) // 1_000_000)


no_match = {'None', 'no_match'}
prok = {'Bacteria', 'Archaea'}
final = d_species.copy()
for g,sk in d_superkingdom.items():
    if sk in no_match:
        continue
    if g in d_species:
        continue
    elif sk not in prok:
        final[g] = sk
    elif g in gs:
        final[g] = gs[g]
    else:
        final[g] = d_predicted_taxid.get(g, 1)
        
                    

for g,p in d_predicted_taxid.items():
    if g not in final:
        final[g] = 1

final = pd.Series(final)
finalstr = final.map(str)
finalnames = finalstr.map(n.names)
finalranks = finalstr.map(n.ranks)

finalframe = pd.DataFrame({'taxid' : finalstr, 'rank' : finalranks, 'name': finalnames})
finalframe.to_csv('taxonomic.final.tsv', sep='\t')
