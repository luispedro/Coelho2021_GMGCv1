import numpy as np
cimport numpy as np

cdef inline int int_min(int a, int b): return a if a <= b else b
ctypedef np.int_t DTYPE_t

def subsample(np.ndarray[DTYPE_t, ndim=1] counts, int N, int copy_data=True, int stepsize=131072):
    '''
    Subsample counts data

    Parameters
    ----------
    counts: ndarray
        Input histogram
    N: int
        Number of elements to sample
    copy_data : boolean,optional
        If True (default), then the algorithm will copy the ``counts`` data to
        a new array to work on it. Otherwise, it will potentially destroy the
        information in it.
    stepsize : int, optional
        Number of elements to sample at each iteration.

    Returns
    -------
    sampled : ndarray
        Histogram of same size as input ``counts`` array.
    '''
    cdef int nexts, i, step
    cdef DTYPE_t acc, di
    cdef np.ndarray[np.int_t, ndim=1] sampled
    cdef np.ndarray[DTYPE_t, ndim=1] new
    if copy_data:
        counts = counts.copy()
    if N > counts.sum():
        raise ValueError("Trying to subsample {} from an array with {} elements.".format(N, counts.sum()))
    new = np.zeros_like(counts)
    while N > 0:
        step = int_min(stepsize, N)
        N -= step
        sampled = np.random.randint(0, counts.sum(), size=step, dtype=np.int)
        sampled.sort()
        acc = 0
        nexts = 0
        for i in range(len(counts)):
            acc += counts[i]
            while sampled[nexts] <= acc:
                nexts += 1
                new[i] += 1
                counts[i] -= 1
                if nexts == step:
                    break
            if nexts == step:
                break
    return new
