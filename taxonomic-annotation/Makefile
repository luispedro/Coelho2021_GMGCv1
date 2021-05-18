SHELL := /bin/bash

NR_DIAMOND_CPUS = 32
INPUT := 1000RefGeneCat
NR_LINES_PER_SPLIT := 100000

final: taxonomic.map

uniref100.fasta:
	wget ftp://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/uniref/uniref100/uniref100.fasta.gz
	gunzip uniref100.fasta.gz

splits/split.done:
	mkdir -p splits
	split -d -l $(NR_LINES_PER_SPLIT) -a 4 $(INPUT).faa splits/$(INPUT).split
	touch $@


uniref100.xml:
	curl ftp://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/uniref/uniref100/uniref100.xml.gz | gunzip > $@

uniref100.taxid: uniref100.xml
	python parse-xml.py > $@

uniref100.wtaxid.dmnd: uniref100.wtaxid.faa
	/g/bork3/home/coelho/work/MOCAT/public/bin/diamond makedb --in $< -d $@

taxdump.tar.gz:
	wget ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz

taxdump: taxdump.tar.gz
	mkdir -p taxdump
	tar xzf taxdump.tar.gz -C $@

splits/%.daa: splits/%
	/g/bork3/home/coelho/work/MOCAT/public/bin/diamond blastp -v --db uniref100.wtaxid --query $< -p $(NR_DIAMOND_CPUS) -a $@ && touch $@.done

$(INPUT).faa.top: splits/$(INPUT).split0000.daa.m8
	python parse-m8.py > $@

$(INPUT).B2.faa: $(INPUT).faa.top
	python prepare-B2.py > $@

splits2/splits2.done: $(INPUT).B2.faa
	mkdir -p splits2
	split -d -l $(NR_LINES_PER_SPLIT) -a 4 $(INPUT).B2.faa splits2/$(INPUT).B2.split.
	touch $@

splits2/%.daa: splits2/%
	/g/bork3/home/coelho/work/MOCAT/public/bin/diamond blastp -v --db uniref100.wtaxid --query $< -p $(NR_DIAMOND_CPUS) -a $@ && touch $@.done

reblast.m8: splits2/$(INPUT).B2.split.0000.daa.done
	(cd splits2 && for d in *.daa; do /g/bork3/home/coelho/work/MOCAT/public/bin/diamond view -a $${d} ; done) > $@

taxonomic.map: reblast.m8 uniref100.taxid 1000RefGeneCat.faa.top
	python final.py ./1000RefGeneCat.faa.top > $@

uniref100.wtaxid.faa: uniref100.fasta uniref100.taxid
	python prepare-B1.py

uniref.taxonomy:
	curl 'http://www.uniprot.org/taxonomy/?sort=&desc=&compress=yes&query=&fil=&format=tab&force=yes&columns=id' | gunzip > $@

GENECAT = /g/bork5/kultima/MOCAT/data/1000RefGeneCat.1 /g/bork5/kultima/MOCAT/data/1000RefGeneCat.2
1000RefGeneCat.faa: $(GENECAT)
	cat $(GENECAT) | /g/bork5/coelho/oceanblast/src/convert > $(INPUT).faa
