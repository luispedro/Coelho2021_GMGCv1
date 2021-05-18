from jug import TaskGenerator
from map_samples_to_genes import map_samples
from biome_rarefaction import rarefy

rarefy = TaskGenerator(rarefy)
map_samples = TaskGenerator(map_samples)
samples_directory = 'samples_20180516'
biomes = [
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
        'wastewater',
        'all',
]

samples_directory = map_samples(index_file='genecats.cold/GMGC.95nr.old-names.faa.dhi',
                               representatives_file='genecats.cold/derived/GMGC.95nr.headers.sorted',
                               relations_file='genecats.cold/GMGC.relationships.txt',
                               output_dir=samples_directory)

for b in biomes:
    rarefy(biome=b, samples_dir=samples_directory, output_dir='outputs/rarefaction_{}'.format(b.replace(' ', '_')), perm_nb=24)
    rarefy(biome=b, samples_dir=samples_directory, output_dir='outputs/singletonfree_rarefaction_{}'.format(b.replace(' ', '_')), perm_nb=24, to_remove='genecats.cold/derived/GMGC.singletons')

