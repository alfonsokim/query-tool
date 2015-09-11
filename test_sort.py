
import random

a = {}
FIVE = range(5)
for i in FIVE:
    temp = list(FIVE)
    random.shuffle(temp)
    #a.append(temp)
    a[i] = temp

print a

## Sort

s = sorted(a.iteritems(), key=lambda x: x[1][4])

print s
