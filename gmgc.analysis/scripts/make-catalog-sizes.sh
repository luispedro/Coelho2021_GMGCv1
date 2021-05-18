#!/usr/bin/env bash
set -e

tab=$(printf "\t")
printf "gene\tlength\tcomplete\n" > outputs/catalog.sizes.txt.tmp

join cold/derived/GMGC.95nr.headers.sorted <(tail -n +2 cold/orf-stats.tsv) "-t$tab" | cut -f1,3,4 "-d$tab"  >> outputs/catalog.sizes.txt.tmp
join cold/derived/GMGC.95nr.headers.sorted <(tail -n +2 cold/freeze12.genes.all_nonENV.renamed.sizes) "-t$tab" | sed 's,$,\tTrue,' >> outputs/catalog.sizes.txt.tmp
mv outputs/catalog.sizes.txt.tmp outputs/catalog.sizes.txt
