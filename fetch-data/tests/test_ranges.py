from ranges import expand, compress
def test_expand():
    samples = 'DRS000003-DRS000008,DRS000011,SRS002116-SRS002118'
    expanded = [
            'DRS000003',
            'DRS000004',
            'DRS000005',
            'DRS000006',
            'DRS000007',
            'DRS000008',
            'DRS000011',
            'SRS002116',
            'SRS002117',
            'SRS002118']
    assert (list(expand(samples)) == expanded)
def test_compress():
    expanded = [
            'DRS000003',
            'DRS000004',
            'DRS000005',
            'DRS000006',
            'DRS000007',
            'DRS000008',
            'DRS000011',
            'SRS002116',
            'SRS002117',
            'SRS002118']
    assert (list(expand(','.join(compress(expanded)))) == expanded)
    assert list(compress(expanded)) == ['DRS000003-DRS000008', 'DRS000011', 'SRS002116-SRS002118']
