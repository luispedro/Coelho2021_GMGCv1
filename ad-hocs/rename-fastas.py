rename = {}
for line in open('GMGC10.rename.table.txt'):
    nname,oname = line.split()
    rename[oname] = nname
    
with open('GMGC.95nr.fna') as ifile, \
   open('GMGC.95nr.fna.renamed', 'wt') as ofile:
    for line in ifile:
        if line[0] == '>':
            line = '>'+rename[line[1:-1]]+'\n'
        ofile.write(line)
        
with open('GMGC.95nr.faa') as ifile, \
   open('GMGC.95nr.faa.renamed', 'wt') as ofile:
    for line in ifile:
        if line[0] == '>':
            line = '>'+rename[line[1:-1]]+'\n'
        ofile.write(line)
        
