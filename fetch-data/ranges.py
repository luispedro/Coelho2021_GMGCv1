import re
def expand(samples):
    for r in samples.split(','):
        if '-' in r:
            start, end = r.split('-')
            pre, start_v = re.match(r'([a-zA-Z]+)(\d+)', start).groups()
            pre2, end_v = re.match(r'([a-zA-Z]+)(\d+)', end).groups()
            assert pre == pre2
            start_vi = int(start_v, base=10)
            end_vi = int(end_v, base=10)

            nchars = len(start_v)
            while start_vi <= end_vi:
                yield (pre+"{{:0{n}}}".format(n=nchars).format(start_vi))
                start_vi += 1
        else:
            yield r


def compress(samples):
    import re
    import itertools
    start = None
    prefix_prev = '0'
    nr_prev = -1
    for s in itertools.chain(sorted(set(samples)), ['MISMATCH000000']):
        # Same entries have sample as "xx samples"
        if re.match(r'\d+ samples', s):
            continue
        prefix, nr = re.match(r'([a-zA-Z]+)(\d+)', s).groups()
        nr = int(nr, base=10)
        if prefix != prefix_prev or nr != nr_prev + 1:
            if start is not None:
                if start != prev:
                    yield "{}-{}".format(start, prev)
                else:
                    yield start
            start = s
        prev = s
        nr_prev = nr
        prefix_prev = prefix
