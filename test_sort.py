
import random

a = []
FIVE = range(5)
for i in FIVE:
    temp = list(FIVE)
    random.shuffle(temp)
    a.append((i, temp))
    #a[i] = temp

print a

## Sort

for i in reversed(FIVE):
    s = sorted(a, key=lambda x: x[1][i])

print s
