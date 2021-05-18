from . import lca

rank_order = [
        'no rank',
        'superkingdom',
        'kingdom',
        'subkingdom',
        'phylum',
        'subphylum',

        'class',
        'subclass',
        'infraclass',

        'infraorder',
        'superorder',


        'order',
        'parvorder',
        'suborder',

        'superfamily',
        'family',
        'subfamily',

        'tribe',
        'subtribe',

        'genus',
        'subgenus',

        'species group',
        'species subgroup',
        'species',
        'subspecies',
        'varietas',
        'forma',
        ]


def load_merged():
    import pandas as pd
    return pd.read_table('taxonomic/taxdump/merged.dmp',
                            sep='|',
                            header=None,
                            usecols=[0,1],
                            index_col=0,
                            squeeze=True).to_dict()


class NCBI(object):
    def __init__(self):
        self.ancestors, self.ranks = lca.load_ncbi_tree()
        self.names = lca.load_ncbi_names()

    def _norm(self, n):
        return str(n)

    def path(self, n):
        return lca.path(self.ancestors, self._norm(n))

    def at_rank(self, n, rank):
        if rank not in rank_order:
            raise ValueError("Unknown rank '{}'".format(rank))
        n = self._norm(n)
        for p in self.path(n):
            if self.ranks[p] == rank:
                return self.names[p]

    def name(self, n):
        return self.names[n]
