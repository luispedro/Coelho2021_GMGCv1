from diskhash import Str2int
from time import time

start = time()

INPUTS = ['data/input.fna']
OUTPUT = 'data/input.fna.dht'

maxlen = 0
n = 0

def iterheaders(inputs):
    for ifname in inputs:
        for line in open(ifname):
            if line[0] != '>': continue
            line = line.rstrip()
            name = line[1:].rstrip().split()[0]
            yield name

for name in iterheaders(INPUTS):
    n += 1
    if maxlen < len(name):
        maxlen = len(name)

print("Found {} genes; maxlen={}".format(n, maxlen))
dh = Str2int('data/input.fna.dht', maxlen, 'rw')
dh.reserve(n)

for ix,name in enumerate(iterheaders(INPUTS)):
    dh.insert(name, ix)
    if (ix + 1) % 10000000 == 0:
        print("inserted {}m.".format((ix+1) // 1000000))

end = time()
print("Inserted {} names in {:.1}s.".format(ix + 1, end - start))

