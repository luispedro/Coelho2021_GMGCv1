import numpy as np 
import subprocess

tests = [
        ([0, 0, 1], [0]),
        ([0, 15, 28], [15]),
        ([0, 28, 15], [28]),
        ([12, 47, 15], [47]),
        ([0, 1, 0,
          0, 1, 0], [1]),
        ([0, 0, 1,
          0, 0, 2], [0]),
        ([0, 0, 1,
          0, 1, 2], [0, 1]),
        ([0, 0, 1,
          0, 1, 2,
          0, 2, 3], [0, 2]),
        ]

for dat, exp in tests:
    open('testing.bin', 'wb').write(np.array(dat, dtype=np.uint32).data)
    r = subprocess.check_output(['./bin/dominant', 'testing.bin']).decode('ascii')
    r = [int(v) for v in r.split()]
    if set(r) != set(exp):
        print("Expected {} saw {} (input was {})".format(exp, r, dat))
