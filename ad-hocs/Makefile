bins:
	mkdir -p bin
	stack install --local-bin-path $$PWD/bin

build:
	stack build

data/freeze12.renamed.100nr.fna: data/freeze12.clean.genes.100nr.fna bins
	./bin/RenameFreeze12Headers data/freeze12.clean.genes.100nr.fna data/freeze12.renamed.100nr.fna data/freeze12.rename.table
