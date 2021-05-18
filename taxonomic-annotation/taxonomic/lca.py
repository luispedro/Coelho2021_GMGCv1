import os
def load_ncbi_tree():
    ancestors = {}
    ranks = {}
    for line in open(os.path.dirname(__file__) + '/taxdump/nodes.dmp'):
        tokens = line.split('\t')
        taxaid = tokens[0]
        ancestorid = tokens[2]
        rank = tokens[4]
        assert taxaid not in ranks
        ranks[taxaid] = rank
        ancestors[taxaid] = ancestorid
    return ancestors, ranks

def load_taxinfo():
    short_names = {}
    long_names = {}
    ranks = {}
    paths = {}
    for i,line in enumerate(open(os.path.dirname(__file__) + '/uniref.taxonomy')):
        if i == 0:
            continue
        tokens = line.rstrip().split('\t')
        taxid = tokens[0]
        short_names[taxid] = tokens[2]
        long_names[taxid] = tokens[5]
        ranks[taxid] = tokens[7]
        paths[taxid] =  ['Root'] + [t.strip() for t in tokens[8].split(';')]
    return short_names, long_names, ranks, paths


def load_ncbi_names():
    names = {}
    for line in open(os.path.dirname(__file__) + '/taxdump/names.dmp'):
        tokens = line.split('\t')
        if tokens[0] not in names or tokens[6] == 'scientific name':
            names[tokens[0]] = tokens[2]
    return names

def path(ancestors, n):
    if n not in ancestors:
        return ['1']
    r = [n]
    while n != '1':
        n = ancestors[n]
        r.append(n)
    return r[::-1]


def lca2(p0, p1):
    i = 0
    while i < min(len(p0), len(p1)) and p0[i] == p1[i]:
        i += 1
    return p0[:i]
def lca(ps):
    ps = ps[:]
    while len(ps) > 1:
        p0 = ps.pop()
        p1 = ps.pop()
        ps.append(lca2(p0, p1))
    return ps[0]
