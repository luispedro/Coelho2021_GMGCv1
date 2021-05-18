import argparse
import os
import sys
import numpy as np

from concurrent import futures
from functools import partial
from glob import glob
from diskhash import StructHash


__author__ = 'glazek'

BIOME_FILE = '/g/bork1/coelho/DD_DeCaF/genecats.cold/biome.txt'


def parse_args():
    desc = """Script calculating cumulative number of unique genes in samples. 
    """
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-n', '--perm-num', help='number of permutations',
                        required=True, dest='perm_nb')
    parser.add_argument('-b', '--biome', help='mapping of sample identifiers to biomes',
                        choices=['all',
                                 'animal gut',
                                 'built-environment',
                                 'cat gut',
                                 'dog gut',
                                 'freshwater',
                                 'human gut',
                                 'human nose',
                                 'human oral',
                                 'human skin',
                                 'human vagina',
                                 'marine',
                                 'mouse gut',
                                 'pig gut',
                                 'soil',
                                 'wastewater'],
                        required=True, dest='biome')
    parser.add_argument('-s', '--samples-dir', help='Directory in which the mapping files are stored',
                        default='sample_genes', dest='samples_dir')
    parser.add_argument('-o', '--output-dir', help='Directory in which the results will be stored',
                        default='results', dest='output_dir')

    return parser.parse_args()


def get_sample_list(samples_dir, biome_samples=None):
    sdir = os.path.abspath(samples_dir)
    all_samples = glob(os.path.join(sdir, '*'))
    if biome_samples:
        return [sample for sample in all_samples if os.path.basename(sample) in biome_samples]
    return all_samples


def get_permutation(sample_list_len):
    return np.random.permutation(sample_list_len)


def run_calculations(i, output_dir, sample_list, sample_list_len, samples_dir, to_remove=None):
    np.random.seed()
    print('\nPermutation number {}'.format(i + 1))
    perm = get_permutation(sample_list_len)
    with open(os.path.join(output_dir, 'perm_{}'.format(i)), 'w') as perm_file:
        perm_file.write('\n'.join([os.path.basename(sample_list[x]) for x in perm])+'\n')
    genes_so_far = set()
    with open(os.path.join(output_dir, 'rarefaction_{}'.format(i)), 'w') as curve_file:
        for sample_count, idx in enumerate(perm):
            if sample_count and not sample_count % 1000:
                sys.stdout.write('.')
                sys.stdout.flush()
                curve_file.flush()
            sample_path = sample_list[idx]
            with open(os.path.join(samples_dir, sample_path), 'r') as s2g:
                for gene in s2g:
                    gene = gene.strip()
                    if to_remove is None or gene not in to_remove:
                        genes_so_far.add(gene)
                curve_file.write('{}\n'.format(len(genes_so_far)))
    del genes_so_far


def rarefy(samples_dir, output_dir, biome, perm_nb, to_remove=None, use_parallel=False):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    print('Getting samples list...')
    biome_samples = None
    if biome != 'all':
        with open(BIOME_FILE) as biome_file:
            biome_samples = [s.split('\t')[0]
                             for s in biome_file.readlines()
                             if s.strip().split('\t')[-1] == biome]
    sample_list = get_sample_list(samples_dir, biome_samples)
    sample_list_len = len(sample_list)
    if to_remove is not None:
        to_remove = frozenset(line.strip() for line in open(to_remove, 'rt'))
    print('{} samples detected'.format(sample_list_len))
    run_calc = partial(run_calculations,
                       output_dir=output_dir,
                       sample_list=sample_list,
                       sample_list_len=sample_list_len,
                       samples_dir=samples_dir,
                       to_remove=to_remove)
    if use_parallel:
        with futures.ProcessPoolExecutor() as pool:
            pool.map(run_calc, range(int(perm_nb)))
    else:
        for i in range(int(perm_nb)):
            run_calc(i)

