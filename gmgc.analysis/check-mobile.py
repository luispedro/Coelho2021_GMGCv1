import numpy as np
from scipy import stats
from glob import glob
from itertools import islice

def build_ctable(g0, g1, total):
    c = np.zeros((2,2))
    c[1,1] = len(g0 & g1)
    c[1,0] = len(g0 - g1)
    c[0,1] = len(g1 - g0)
    c[0,0] = total - c.sum()
    return c


TOTAL = 311197450.
def test_group(group, travelers, name, total=TOTAL):
    print(f"Test {name}")
    print("Fraction of genes that travel {:1%}".format(len(travelers)/total))
    print("Fraction of genes that travel ({name} genes only) {:1%}".format(len(travelers&group)/len(group), name=name))
    print("Fraction of {name} genes {:.1%}".format(len(group)/total, name=name))
    print("Fraction of {name} genes within travelers {:.1%}".format(len(group&travelers)/len(travelers), name=name))
    print("Association test")
    print(stats.fisher_exact(build_ctable(group, travelers, total)))

ABRs = set(line.split()[0] for line in islice(open('cold/annotations/GMGC.card_resfam_updated.out.r'), 1, None) if not line.lstrip().startswith('SYNERGY'))
mobile = set(line.split()[0] for line in open('cold/annotations/e5_besthit_DDE_TRdb_filtered_cellular_duplicates.out'))


travelers = set(line.split()[0] for line in open('outputs/travelers.txt'))


test_group(mobile, travelers, 'mobile')
print("\n\n")
test_group(ABRs, travelers, 'ABR')

human_genes = set(line.strip() for line in open('outputs/GMGC.human_gut.95nr.headers.txt'))

print("\n\nHUMAN GUT ONLY")
test_group(mobile& human_genes, travelers&human_genes, 'mobile', total=len(human_genes))
test_group(ABRs& human_genes, travelers&human_genes, 'abrs', total=len(human_genes))
