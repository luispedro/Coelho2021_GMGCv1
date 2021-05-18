import pandas as pd
interesting = set(line[1:].strip() for line in open("data/TestNotInCatalog.faa") if line[0] == '>')
pairs = []
for i,line in enumerate(open('../cold/GMGC.relationships.txt')):
    a,_,b = line.split()
    if a in interesting:
        pairs.append((a,b))
    if (i+1) % 1000000 == 0:
        print("Analyzed {}m lines".format((i+1)/1000000))
interesting.update(b for _,b in pairs)

annotations = {}
for line in open('../cold/annotations/GMGC.95nr.emapper.annotations'):
    if line[0] == '#':continue
    tokens = line.strip('\n').split('\t')
    if tokens[0] in interesting:
        annotations[tokens[0]] = tokens
nannotations = {}
for line in open('data/TestNotInCatalog.emapper.annotations'):
    if line[0] == '#':continue
    tokens = line.strip('\n').split('\t')
    nannotations[tokens[0]] = tokens

eq = 0
neq = 0


card = pd.read_table('../cold/annotations/GMGC.card_resfam_updated.out.r', index_col=1)
card2 = pd.read_table('/g/bork1/forslund/f11_annotations/luisnotcat.card_resfam.out', index_col=1)


card = pd.concat([card,card2])
carded = set(card.index)
for a,b in pairs:
    if a in carded and b in carded:
        if card.loc[a].AROs == card.loc[b].AROs:
            eq += 1
        else:
            neq += 1



carsnp = pd.read_table('/g/bork1/forslund/f11_annotations/luiscat.snp.out', index_col=0)


carsnp.set_index('Gene', inplace=True)
carsnp2 = pd.read_table('/g/bork1/forslund/f11_annotations/luisnotcat.snp.out', index_col=0)
carsnp2.set_index('Gene', inplace=True)




carsnp = carsnp.append(carsnp2)

eq = 0
neq = 0
carded = set(carsnp.index)
for a,b in pairs:
    if a in carded and b in carded:
        if cardsnp.loc[a].AROs == cardsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1



for a,b in pairs:
    if a in carded and b in carded:
        if cardsnp.loc[a].AROs == cardsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1

for a,b in pairs:
    if a in carded or b in carded:
        if cardsnp.loc[a].AROs == cardsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1


for a,b in pairs:
    if a in carded or b in carded:
        if cardsnp.loc[a].AROs == cardsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1

carded = set(carsnp.index)

carsnp = pd.read_table('/g/bork1/forslund/f11_annotations/luiscat.snp.out', index_col=0)
carsnp.set_index('Gene', inplace=True)


carsnp = pd.read_table('/g/bork1/forslund/f11_annotations/luiscat.snp.out', index_col=0)

carsnp.set_index('ID', inplace=True)
carsnp2 = pd.read_table('/g/bork1/forslund/f11_annotations/luisnotcat.snp.out', index_col=0)
carsnp2.set_index('ID', inplace=True)
carsnp = carsnp.append(carsnp2)
carded = set(carsnp.index)

for a,b in pairs:
    if a in carded or b in carded:
        if cardsnp.loc[a].AROs == cardsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1

for a,b in pairs:
    if a in carded or b in carded:
        if carsnp.loc[a].AROs == carsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1

for a,b in pairs:
    if a in carded and b in carded:
        if carsnp.loc[a].AROs == carsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1



eq = 0
neq = 0
for a,b in pairs:
    if a in carded and b in carded:
        if carsnp.loc[a].AROs == carsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1



mis = []
for a,b in pairs:
    if a in carded and b in carded:
        if carsnp.loc[a].AROs == carsnp.loc[b].AROs:
            eq += 1
        else:
            neq += 1
            mis.append((a,b))

