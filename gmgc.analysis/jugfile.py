from jug import TaskGenerator

from biomes_count import generate_biome_count, write_out, find_travelers, count_shuffled_travelers, get_pair_counts
from subcatalogs import build_subcatalog, update_catalog_names
from gene_families import count_gene_names

find_travelers = TaskGenerator(find_travelers)
count_shuffled_travelers = TaskGenerator(count_shuffled_travelers)
build_subcatalog = TaskGenerator(build_subcatalog)
update_catalog_names = TaskGenerator(update_catalog_names)
generate_biome_count = TaskGenerator(generate_biome_count)
get_pair_counts = TaskGenerator(get_pair_counts)
write_out = TaskGenerator(write_out)

@TaskGenerator
def compute_gc():
    from collections import Counter
    from safeout import safeout
    oname = 'outputs/gc.txt'
    with safeout(oname, 'wt') as output:
        for line in open('./cold/GMGC.95nr.fna'):
            if line[0] == '>':
                gene = line[1:].strip()
            else:
                c = Counter(line)
                n = len(line)
                output.write('{}\t{}\n'.format(gene, (c['G'] + c['C'])/n))
    return oname

@TaskGenerator
def fraction_singleton(counts, counts_ns):
    r = {}
    biomes = set()
    for k in counts:
        biomes.update(k)
    for b in biomes:
        total = 0
        for k, v in counts.items():
            if b in k:
                total += v
        ns = 0
        for k,v in counts_ns.items():
            if b in k:
                ns += v
        r[b] = 1. - ns/total
    return r

@TaskGenerator
def plot_singleton_split(fs, oname):
    from matplotlib import use
    use('Agg')
    from matplotlib import pyplot as plt

    import seaborn as sns
    import numpy as np

    fig,ax = plt.subplots()
    bar_ns = []
    biomes = []
    for b in sorted(fs, key=fs.get):
        bar_ns.append(fs[b])
        biomes.append(b)

    ax.bar(np.arange(len(biomes)), 1-np.array(bar_ns), .8, bottom=bar_ns)
    ax.bar(np.arange(len(biomes)), bar_ns, .8)
    ax.set_xticks(np.arange(len(biomes)))
    ax.set_xticklabels(biomes)
    sns.despine(fig, offset=4)
    fig.tight_layout()
    fig.savefig(oname)

SUBCATALOGS = [
            'human gut',
            'human oral',
            'human skin',
            'human nose',
            'human vagina',
            'soil',
            'freshwater',
            'built-environment',
            'cat gut',
            'dog gut',
            'pig gut',
            'marine',
            'mouse gut',
            'wastewater',
            ]
jug_execute(['./scripts/make-catalog-sizes.sh'])

@TaskGenerator
def count_by_biome(counts):
    from collections import defaultdict
    single_counts = defaultdict(int)
    for k,v in counts.items():
        for b in k:
            single_counts[b] += v
    return dict(single_counts)

@TaskGenerator
def count_annotated_by_biome(travelers):
    from collections import defaultdict, Counter
    import pandas as pd
    annotation_file = 'cold/annotations/GMGC.95nr.emapper.annotations'
    annotated = set(line.split('\t', 1)[0] for line in open(annotation_file) if line[0] != '#')
    annotated_by_biome = defaultdict(int)
    for line in open(travelers):
        tokens = line.strip().split('\t')
        if tokens[0] in annotated:
            for tk in tokens[1:]:
                annotated_by_biome[tk] += 1
            annotated.remove(tokens[0])
    annotated_by_sample = Counter(g.split('_')[0] for g in annotated)
    biome = pd.read_table('./cold/biome.txt', index_col=0)
    for s,[b] in biome.iterrows():
        annotated_by_biome[b] += annotated_by_sample.get(s, 0)
    return dict(annotated_by_biome)

@TaskGenerator
def rename_travelers(iname, oname):
    import rename
    r = rename.Renamer()
    with open(oname, 'wt') as out:
        for line in open(iname):
            tokens = line.rstrip('\n').split('\t')
            tokens[0] = r.new_name(tokens[0])
            out.write('\t'.join(tokens))
            out.write('\n')
    return oname

@TaskGenerator
def stats_shuffled(shuffled):
    from scipy import stats
    import numpy as np
    N = 18145135
    _, p = stats.shapiro(shuffled)
    mu = np.mean(shuffled)
    std = np.sqrt(np.var(shuffled, ddof=1))
    delta = (mu - N) / std
    with open('outputs/travelers_pvalue.txt', 'wt') as output:
        output.write(f'Shapiro test of normality: p-value: {p}\n')
        output.write(f'Mean = {mu}\n')
        output.write(f'Std.dev. = {std}\n')
        output.write(f'Delta = {delta}\n')

counts = generate_biome_count()
pairs = get_pair_counts(counts)
counts1 = count_by_biome(counts)
write_out(counts, 'outputs/gene-biomes.txt')

counts_ns = generate_biome_count(False)
write_out(counts_ns, 'outputs/gene-biomes-no-singletons.txt')
frac_singles = fraction_singleton(counts, counts_ns)
plot_singleton_split(frac_singles, 'plots/singleton-split.svg')

travelers = find_travelers('outputs/travelers.txt', 'outputs/freeze12.assigned.txt')
shuffled = [count_shuffled_travelers(i) for i in range(32)]
stats_shuffled(shuffled)
rename_travelers(travelers, 'outputs/travelers.renamed.txt')

annotated_by_biome = count_annotated_by_biome(travelers)

for biome in SUBCATALOGS:
    onames = build_subcatalog(biome, travelers, 'outputs/freeze12.assigned.txt')
    update_catalog_names(onames)

compute_gc()

name_counts = count_gene_names()
