import pickle
rename = pickle.load(open('cold/rename.pkl', 'rb'))
rare = [rename[line.strip()] for line in open('tables/rare-genes.txt')]
rare.sort()
with open('cold/rare-genes.txt', 'wt') as output:
    for g in rare:
        output.write(f'{g}\n')
