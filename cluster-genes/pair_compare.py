def compare_pair(seq0, seq1, cache=[]):
    '''
    Compare a pair of nucleotide strings

    Parameters
    ----------
    seq0, seq1: str
        Query inputs

    Returns
    -------
        relationship: char
            =: equality
            C: contained
            D: contained (reversed [i.e., second elem is CONTAINED in first])
            M: matched
            W: matched (reverse [i.e., second represents first])
            E: equivalent
            X: mismatched
    '''
    import skbio.alignment
    switched = False
    if len(seq1) > len(seq0):
        seq0,seq1 = seq1,seq0
        switched = True
    if str(seq1) in str(seq0):
        if len(seq1) == len(seq0):
            return '='
        elif switched: # seq0 contained in q1
            return 'C'
        else:
            return 'D' # q1 contained in seq0
    # Default is match equals 2 points & mismatch -3
    # 95% ID : len * (95% * 2 + 5% * (-3))
    #        = len * (1.9 - .15)
    #        = len * 1.75
    min_score = len(seq1) * 1.75
    if cache and seq0 == cache[0]:
        sw = cache[1]
    else:
        sw = skbio.alignment.StripedSmithWaterman(seq0,
                                            score_only=True,
                                            suppress_sequences=True)
        cache.clear()
        cache.extend([seq0, sw])
    score = sw(seq1)['optimal_alignment_score']
    if score is not None and score >= min_score:
        if score >= len(seq0) * 1.75:
            return 'E'
        return ('M' if switched else 'W')
    return 'X'

def compare_lines(fafile0, fafile1, lines):
    import fasta_reader
    data0 = fasta_reader.IndexedFastaReader(fafile0, use_mmap=True)
    data1 = fasta_reader.IndexedFastaReader(fafile1, use_mmap=True)
    res = []
    data_cache = {}
    cache = []
    for line in lines:
        q0,q1 = line.rstrip().split('\t')[:2]
        if q0 == q1:
            continue
        if len(data_cache) > 100000:
            data_cache = {}
        if q0 not in data_cache:
            data_cache[q0] = data0.get(q0).decode('ascii')
        d0 = data_cache[q0]
        if q1 not in data_cache:
            data_cache[q1] = data1.get(q1).decode('ascii')
        d1 = data_cache[q1]
        code = compare_pair(d0, d1, cache)
        if code != 'X':
            res.append((code, q0, q1))
    return res

