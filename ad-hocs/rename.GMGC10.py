import pandas as pd
gene_names = pd.read_table('annotations/GMGC.95nr.emapper.annotations', header=None, index_col=0, usecols=[0,4])
gene_names = gene_names.squeeze()
lens = gene_names.map(lambda s: len(str(s)))
gn = gene_names.dropna().to_dict()
with open('GMGC10.rename.table.txt', 'wt') as output:
    for i,line in enumerate(open('derived/GMGC.95nr.headers.sorted')):
        oname = line.strip()
        name = gn.get(oname, 'UNKNOWN')
        if len(name) > 10:
            name = 'UNKNOWN'
        n = '{:09}'.format(i)
        new_name ='GMGC10.'+n[:3]+'_'+n[3:6]+'_'+n[6:]+'.'+name
        output.write(f'{new_name}\t{oname}\n')
