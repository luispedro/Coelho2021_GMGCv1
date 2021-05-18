from jug import TaskGenerator

@TaskGenerator
def check_emapper_results(only_cross_complete=False):
    import gzip
    interesting = set(line[1:].strip() for line in gzip.open("data/TestNotInCatalog.faa.gz", 'rt') if line[0] == '>')
    if only_cross_complete:
        import pandas as pd
        metaorf = pd.read_table('../cold/orf-stats.tsv', index_col=0)
        complete = set(metaorf.query('complete').index)
        interesting = interesting - complete
    pairs = []
    for i,line in enumerate(open('../cold/GMGC.relationships.txt')):
        a,_,b = line.split()
        if a in interesting and (not only_cross_complete or b in complete):
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
    for line in gzip.open('data/TestNotInCatalog.emapper.annotations.gz', 'rt'):
        if line[0] == '#':continue
        tokens = line.strip('\n').split('\t')
        nannotations[tokens[0]] = tokens

    eq = 0
    neq = 0

    def extract_bactNOG(ann):
        maybe = [b for b in ann[9].split(',') if b.endswith('@bactNOG')]
        if maybe:
            return maybe[0]
    for a,b in pairs:
        if b not in annotations or a not in nannotations:
            continue
        if extract_bactNOG(annotations[b]) != extract_bactNOG(nannotations[a]):
            neq += 1
        else:
            eq += 1
    return neq/(eq+neq)

@TaskGenerator
def write_results(full, frag2complete):
    with open('results.txt', 'wt') as output:
        output.write("Match gene/unigene (overall): {:.2%}\n".format(1-full))
        output.write("Match gene/unigene (fragment->complete): {:.2%}\n".format(1-frag2complete))
full = check_emapper_results()
frag2complete = check_emapper_results(True)

write_results(full, frag2complete)
