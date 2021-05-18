def groups_of(iterable, n):
    """Collect data into fixed-length chunks or blocks

    Example:
        groups_of('ABCDEFG', 3) --> ABC DEF G"
    """
    cur = []
    for it in iterable:
        cur.append(it)
        if len(cur) == n:
            yield cur
            cur = []
    if cur:
        yield cur

def maybe_bz2_open(f, mode='rt'):
    if f.endswith('.bz2'):
        import bz2
        return bz2.open(f, mode)
    return open(f, mode)



def make_temp_copy(f, odir):
    import shutil
    from os import path
    from jug.utils import sync_move
    import tempfile
    oname = path.join(odir, path.basename(f))
    if path.exists(oname):
        return oname

    t = tempfile.NamedTemporaryFile(dir=odir, delete=False)
    t.close()

    shutil.copy(f, t.name)
    sync_move(t.name, oname)
    return oname

