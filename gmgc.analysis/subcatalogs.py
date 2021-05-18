from safeout import safeout

def build_subcatalog(biome, travelers, fr12assignments):
    import pandas as pd
    sample2biome = pd.read_table('./cold/biome.txt', index_col=0)
    sample2biome = sample2biome.to_dict()['biome']
    sample2biome['Fr12'] = 'freeze12'
    fr12 = pd.read_table('./outputs/freeze12.assigned.txt', index_col=0,header=None).to_dict()[1]
    blacklist = set(line.strip() for line in open('cold/redundant.complete.sorted.txt'))
    interesting = set()
    for line in open('outputs/travelers.txt'):
        tokens = line.strip().split('\t')
        if biome in tokens[1:]:
            interesting.add(tokens[0])
    interesting -= blacklist
    oname = 'outputs/subcatalogs/GMGC.{}.95nr.old-names.fna'.format(biome.replace(' ', '-'))
    n = 0
    with safeout(oname, 'wt') as output:
        active = False
        for line in open('cold/GMGC.95nr.old-names.fna'):
            if line[0] == '>':
                gene = line[1:].strip()
                active = (gene in interesting or
                          sample2biome[gene.split('_')[0]] == biome or
                          fr12.get(gene) == biome)
                n += active
            if active:
                output.write(line)

    print("Wrote {}: {} genes".format(oname, n))
    return oname

def update_catalog_names(ifile):
    assert '.old-names' in ifile
    import diskhash
    gene2index = diskhash.Str2int('/local/coelho/genecats.cold/gene2index.s2i', 24, 'r')
    gene_names = open('/local/coelho/GMGC10.names.sorted.32', 'r')
    def new_name(n):
        ix = gene2index.lookup(n)
        gene_names.seek(32*ix)
        return gene_names.readline().strip()
    ofile = ifile.replace('.old-names','')
    with open(ifile) as ifa, open(ofile, 'wt') as ofa:
        for line in ifa:
            if line[0] == '>':
                n = new_name(line[1:-1])
                ofa.write('>'+n+'\n')
            else:
                ofa.write(line)
