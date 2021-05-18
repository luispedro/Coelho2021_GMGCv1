import lxml.etree as ET
import gzip
from sys import argv

for event, elem in ET.iterparse(gzip.open(argv[1], 'rb')):
    if event == 'end' and elem.tag == '{http://uniprot.org/uniprot}entry':
        t = elem.xpath("u:organism/u:dbReference[@type='NCBI Taxonomy']" , namespaces={'u':'http://uniprot.org/uniprot'})
        if len(t) == 0:
            continue
        if len(t) != 1:
            raise ValueError("SHIT")
        t = t[0]
        ncbi = t.get('id')
        prot = elem.xpath('u:name', namespaces={'u':'http://uniprot.org/uniprot'})[0].text
        print("{}\t{}".format(prot, ncbi))

        elem.clear()
        # Also eliminate now-empty references from the root node to elem
        for ancestor in elem.xpath('ancestor-or-self::*'):
            while ancestor.getprevious() is not None:
                del ancestor.getparent()[0]
