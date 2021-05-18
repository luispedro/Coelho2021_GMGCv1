from Bio import SeqIO
def read_fasta(fa):
    for rec in SeqIO.parse(fa, format='fasta'):
        yield rec.id, str(rec.seq)
with open('complete.only/GMGC10.sam.header', 'wt') as output:
    for n,seq in read_fasta(fa):
        output.write('@SQ\tSN:{n}\tLN:{len}\n'.format(n=n, len=len(seq)))
