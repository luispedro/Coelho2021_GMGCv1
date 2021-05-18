import numpy as np

alphabet = 'C FY W ML IV G P A T S N H QE D R K'.split()
encoding = [0 for _ in range(256)]
for i,a in enumerate(alphabet):
    for c in a:
        encoding[ord(c)] = i
print(np.array(encoding))
