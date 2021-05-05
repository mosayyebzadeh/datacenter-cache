
import csv, sys
import matplotlib.pyplot as plt

data = {}
size = {}

#traceFile = './trace003GET.csv'

traceFile = str(sys.argv[1])
sizeOfOnce = 0
totalSize = 0
with open(traceFile) as csvFile:
    csvReader = csv.reader(csvFile, delimiter=',', skipinitialspace=True)
    for row in csvReader:
        key = str(row[3])
        if key in data.keys():
            data[key] += 1
        else:
            data.update({key: 1})
            size.update({key: float(row[2])})
            sizeOfOnce += size[key]
        totalSize += size[key]



maxim = 0
maximSize = 0
keyCount  = 0
accessOnceKey = 0
totalCount = 0
for key in data.keys():
    keyCount += 1
    totalCount += data[key]

    if data[key] == 1:
        accessOnceKey += 1

    if data[key] > maxim:
        maxim = data[key]
        maxKey = key

    if size[key] > maximSize:
        maximSize = size[key]
        maxSizeKey = key


print("Total key and access are %d %d" %(keyCount, totalCount))
print("average access is %d" %(totalCount/keyCount))
print("Access once is %d" %(accessOnceKey))
print("Total unique size is %d" %(sizeOfOnce))
print("Total unique (GB) size is %d" %(sizeOfOnce/(1024)))
print("Total size is %d" %(totalSize))
print("Total size (GB) is %d" %(totalSize/(1024)))
print("maximum object size is %s: %d" %(maxSizeKey, size[maxSizeKey]))
print("maximum access count is %s: %d" %(maxKey, data[maxKey]))

"""
stat = {1: 0}
for key in data.keys():
    if data[key] in stat.keys():
        stat[data[key]] += 1
    else:
        stat.update({data[key]: 1})

sortedStat = {}
for key in sorted (stat.keys()):
    sortedStat.update({key: stat[key]})
#print(sortedStat)

x = sortedStat.keys()
y = sortedStat.values()
plt.plot(x, y)
plt.yscale("log")
plt.ylabel('Object Count (log scale)')
plt.xlabel('Access Count')
plt.savefig('graph_log.pdf') 
plt.savefig('graph_log.png') 

plt.loglog(x, y)
plt.ylabel('Object Count (loglog scale)')
plt.xlabel('Access Count')
plt.savefig('graph_loglog.pdf') 
plt.savefig('graph_loglog.png') 
"""

