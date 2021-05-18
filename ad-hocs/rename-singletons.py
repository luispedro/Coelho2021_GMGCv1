import pickle 
rename = pickle.load(open('cold/rename.pkl', 'rb'))

with open('cold/derived/GMGC10.singletons', 'wt') as output:
    for line in open('cold/derived/GMGC.singletons'):
        line = rename[line.strip()]
        output.write(f'{line}\n')
