import argparse
import os
import sys

from collections import defaultdict
from diskhash import StructHash

__author__ = 'glazek'


def parse_args():
    desc = """Script creating mapping from sample to representative genes.
    For each sample a file will be created in which representative gene identifiers will be stored one per line.
    
    NOTE: The script will run MUCH faster if the relations file is sorted by the last column."""
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-i', '--index', help='Diskhash-based index of the fasta file with representative genes',
                        required=True, dest='index_file')
    parser.add_argument('--rep', help='List of representative genes',
                        required=True, dest='representatives')
    parser.add_argument('-r', '--relations', help='Sorted file with relations between the genes',
                        required=True, dest='relations')
    parser.add_argument('-o', '--output-dir', help='Directory in which the mapping files will be stored',
                        default='sample_genes', dest='output_dir')

    return parser.parse_args()


def map_samples(relations_file, representatives_file, index_file, output_dir):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    print('Sample->[rep_gene] mappings will be stored in {}'.format(output_dir))
    print('Loading index from {}'.format(index_file))
    index = StructHash(index_file, 0, '2l', 'r')
    print('Index loaded!')
    sample2gene = defaultdict(list)

    def write_to_file(sample, gene_list):
        with open(os.path.join(output_dir, sample), 'a') as sample_mapping:
            sample_mapping.writelines(gene_list)

    def proceed(ing, repg):
        s, _ = ing.split('_')
        if s == 'Fr12': return
        sample2gene[s].append(repg + '\n')
        if len(sample2gene[s]) > 2048:
            write_to_file(s, sample2gene[s])
            del sample2gene[s]

    print('Mapping representatives...')
    with open(representatives_file) as representatives:
        for gene in representatives:
            gene = gene.strip()
            proceed(gene, gene)
    print('Representatives mapped.')

    print('Relationships file processing...')
    with open(relations_file) as relations:

        for i, line in enumerate(relations):
            if i and not i % 100000:
                sys.stdout.write('.')
                sys.stdout.flush()
            in_gene, _, rep_gene = line.strip().split()
            if not index.lookup(in_gene):
                proceed(in_gene, rep_gene)


    print('\nRelationships file processed.')

    for sample, gene_list in sample2gene.items():
        write_to_file(sample, gene_list)
    print('\nDone!')
    return output_dir


def main():
    args = parse_args()

    map_samples(relations_file=args.relations,
                representatives_file=args.representatives,
                index_file=args.index_file,
                output_dir=args.output_dir)


if __name__ == '__main__':
    main()
