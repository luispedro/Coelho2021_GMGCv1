def get_ncpus():
    from os import environ
    for ev in ['OMP_NUM_THREADS', 'Q_CORES', 'Q_CORE']:
        if ev in environ:
            return int(environ[ev].strip())
    for ev in ['LSB_MCPU_HOSTS']:
        if ev in environ:
            break
    else:
        return 1
    tokens = environ[ev].strip().split()
    if len(tokens) > 2:
        raise SystemError("Cannot handle this type of environment ({}='{}')".format(ev, environ[ev]))
    return int(tokens[1])

if __name__ == '__main__':
    print('Running with {} CPUS.'.format(get_ncpus()))
