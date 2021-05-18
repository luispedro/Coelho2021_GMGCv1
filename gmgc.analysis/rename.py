import diskhash

class Renamer(object):
    def __init__(self):
        self.gene2index = diskhash.Str2int('/local/coelho/genecats.cold/gene2index.s2i', 24, 'r')
        self.gene_names = open('/local/coelho/GMGC10.names.sorted.32', 'r')

    def new_name(self, n):
        ix = self.gene2index.lookup(n)
        self.gene_names.seek(32*ix)
        return self.gene_names.readline().strip()
