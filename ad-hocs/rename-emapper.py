from safeout import safeout
import pandas as pd
rename = {}
for line in open('cold/GMGC10.rename.table.txt'):
    nname,oname = line.split()
    rename[oname] = nname
with safeout('cold/annotations/GMGC10.emapper.annotations.tsv', 'wt') as output:
    for line  in open('cold/annotations/GMGC.95nr.emapper.annotations'):
        name,rest = line.split('\t', 1)
        name = rename[name]
        output.write(f'{name}\t{rest}')
