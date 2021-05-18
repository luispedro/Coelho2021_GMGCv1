import pandas as pd

flows = {
   'upflow': 'Unigene',
   'upflow.90': 'Clusters',
   'upflow.gf': 'Families',
   }
with pd.ExcelWriter('preprocessed/SourceDataGeneSharing.xlsx') as out:
    for fname,k in flows.items():
        data = pd.read_table('tables/{}.tsv'.format(fname), index_col=0)
        data.to_excel(out, sheet_name=k)
