from safeout import safeout
rename = {}
for line in open('cold/GMGC10.rename.table.txt'):
    nname,oname = line.split()
    rename[oname] = nname
rename['ID'] = 'ID'
rename['SYNERGY'] = 'SYNERGY'

with safeout('cold/annotations/GMGC10.card_resfam_updated.tsv', 'wt') as output:
    for line in open('cold/annotations/GMGC.95nr.card_resfam_updated.out.r'):
        tokens = line.split('\t')
        if tokens[2] in rename:
            tokens[2] = rename[tokens[2]]
        else:
            tokens[2]= '-'.join(map(rename.get, tokens[2].split('-')))
        output.write('\t'.join(tokens))
