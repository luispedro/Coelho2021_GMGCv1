import subprocess
from safeout import safeout
from jug import TaskGenerator


@TaskGenerator
def select(r):
    from random import random
    selected = []
    for line in open('./complete.headers'):
        if random() < r:
            selected.append(line.strip())

    selected.sort()
    with safeout('random0001.txt', 'wt') as output:
        for line in selected:
            output.write(line)
            output.write('\n')
    return 'random0001.txt'

@TaskGenerator
def filter_catalog(r):
    oname = 'testquery.txt'
    with safeout(oname) as output:
        subprocess.check_call(['comm', '-23', r, '/g/bork1/coelho/DD_DeCaF/genecats.cold/derived/GMGC.95nr.headers.sorted'], stdout=output.tfile)
    return oname


@TaskGenerator
def extract_fas(t):
    oname = 'TestNotInCatalog.faa'
    subprocess.check_call(['../bin/RetrieveFastaSeqs', '-k', t, '-i', '/g/bork1/coelho/DD_DeCaF/genecats.cold/GMGC.100nr.faa', '-o', oname])
    return oname


r = select(.0001)
t = filter_catalog(r)
fa = extract_fas(t)

