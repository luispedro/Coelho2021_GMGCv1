# Reformat the card_resfam_updated table to an NGLess functional_map table
import pandas as pd
import pickle

card_resfam = pd.read_table('cold/annotations/GMGC.95nr.card_resfam_updated.out.r')

for c in ['Taxid', 'Bioproject']:
    assert not len(card_resfam[c].dropna())
    card_resfam = card_resfam.drop(c,axis=1)
rename = pickle.load(open('cold/rename.pkl', 'rb'))
card_resfam['ID'] = card_resfam['ID'].map(rename.get)
card_resfam = card_resfam.dropna(subset=['ID']).rename(columns={'ID': 'Unigene'}).set_index('Unigene')
card_resfam['AROs'] = card_resfam.AROs.map(lambda ar: ar.replace(' ',''))
card_resfam['Drugs'] = card_resfam.Drugs.map(lambda ar: (ar.replace(' ','') if type(ar) == str else ar))

with open('cold/annotations/GMGC10.card_resfam.updated.tsv', 'wt') as out:
    out.write('# GMGC 1.0 CARD/Resfam annotations reformated for NGLess\n')
    out.write('#')
    card_resfam.to_csv(out, sep='\t')

