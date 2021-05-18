import pandas as pd
sizes = {}
for line in open('cold/GMGC.95nr.fna'):
    if line[0] == '>':
        gene = line[1:-1]
    else:
        sizes[gene] = len(line)-1
sizes = pd.Series(sizes)
pd.DataFrame({'sizes': sizes}).to_csv('tables/GMGC.95nr.sizes', sep='\t')
