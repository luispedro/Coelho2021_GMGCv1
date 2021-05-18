def group(it, n):
    it = iter(it)
    block = []
    for elem in it:
        block.append(elem)
        if len(block) == n:
            yield block
            block = []
    if block:
        yield block
