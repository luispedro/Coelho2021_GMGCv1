import pandas as pd

ecoli_pos = pd.read_table('gmgc_selection/ecoli.absrel.pval.detections.with_header.tsv', index_col=1)
fscore = pd.read_table('tables/fscore.prevalence.update.tsv.gz', index_col=0)
unig = pd.read_table('tables/metasel.tsv', index_col=0)
unig30 = pd.read_table('tables/selection_complete30_w_prev.tsv', index_col=0)
ecoli = pd.read_table('./tables/Ecoli.selection_per_site.detection_number.txt',
        sep=' ',
        index_col=0)

with pd.ExcelWriter('preprocessed/SourceData_Selection.xlsx') as out:
    fscore.to_excel(out, sheet_name='Neighbourhood conservation')
    unig.to_excel(out, sheet_name='Unigene selection')
    unig30.to_excel(out, sheet_name=r'Unigene selection (30% families)')
    ecoli_pos.to_excel(out, sheet_name=r'E coli')
    ecoli.to_excel(out, sheet_name=r'E coli (per site)')
