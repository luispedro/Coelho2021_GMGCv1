from sys import argv

ifile = argv[1]
ofile = argv[2]
spaces = ''.join([' ' for _ in range(31)]) + '\n'

with open(ofile, 'wt') as output:
    for line in open(ifile):
         if line[0] == '>':
             gene = line[1:-1]
             output.write(gene + spaces[len(gene):])

