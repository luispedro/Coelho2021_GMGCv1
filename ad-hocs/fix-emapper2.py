import safeout
import gzip
names = '''query_name
seed_eggNOG_ortholog
seed_ortholog_value
seed_ortholog_score
Predicted_taxonomic_group
Predicted_protein_name
Gene_Ontology_terms 
EC_number
KEGG_ko
KEGG_Pathway
KEGG_Module
KEGG_Reaction
KEGG_rclass
BRITE
KEGG_TC
CAZy 
BiGG_Reaction
tax_scope
eggNOG_OGs 
bestOG
COG_Functional_Category
eggNOG_free_text'''.split()

with safeout.safeout('cold/annotations/GMGC10.emapper2.annotations.tsv.fixed.gz', 'wb') as s_out:
    with gzip.open(s_out, 'wt') as out:
        with gzip.open('cold/annotations/GMGC10.emapper2.annotations.tsv.gz', 'rt') as ifile:
            for line in ifile:
                if line[0] != '#' or not line.startswith('#query'):
                    out.write(line)
                else:
                    out.write('#')
                    out.write('\t'.join(names))
                    out.write('\n')
