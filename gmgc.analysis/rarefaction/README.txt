# Summary:
Rarefaction curves were calculated for 24 random permutations of samples from
each biome. For every permutation a cumulative total number of distinct ORFs
was computed. Code used for calculating rarefaction curves can be found in
/rarefaction directory.

# Approach (more technical description of the solution):

For each sample from which at least one gene was assembled a file with a list
of representative genes is created.

Files are populated first with the representatives coming from given sample, then with genes from relationships file (for each row like
sampleA_geneX rel sampleB_geneY
gene sampleB_geneY is added to a file corresponding to sample sampleA, unless sampleA_geneX is a representative itself or another representative for this gene was already added)

Once the lists of representative genes per sample are ready the rarefaction curves are calculated.

For given biome and number of permutations the cumulative number of distinct is calculated.

For each permutation a separate diskhash index is created. For every sample in
permutation, genes listed for that sample are added to the index and the size
of index is written to the output file after processing of each single sample.

Diskhash indices are removed at the end of processing.

# Commands run:

jug execute

# Results:
In results directory there are subdirectories for each biome, each containing the 24 permutations and corresponding cumulative sums.


# Notes:
There are some things hardcoded in the scripts, so in case they should be published, I guess it will need a slight adjustments.
