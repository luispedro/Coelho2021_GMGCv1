import pandas as pd
files = {
        'Unigene': 'tables/genes.prevalence.1m.no-dups.hists.txt',
        'Protein Clusters (90% AAI)': 'tables/GMGC.90nr.gene.hists.1m.no-dups.txt',
        'Protein Families (20% AAI)': 'tables/GMGC.gf.gene.hists.1m.no-dup.txt',
        'Unigene, complete-only': 'tables/GMGC10.histogram.1m.no-dups.complete-only.tsv',
        'Protein Clusters (90% AAI), complete-only': 'tables/GMGC10.histogram.1m.no-dups.clusters90.only-w-complete.tsv',
        'Protein Families (20% AAI), complete-only': 'tables/gf-histogram-only-genes-in-complete.tsv',
    }
with pd.ExcelWriter('preprocessed/SourceData_Prevalence.xlsx') as out:
    for k, fname in files.items():
        pd.read_table(fname, index_col=0, ).to_excel(out, sheet_name=k)
