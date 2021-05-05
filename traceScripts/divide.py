import sys
import csv


traceFile = str(sys.argv[1])
path = str(sys.argv[2])
outputFileCount = int(sys.argv[3])

fd = []
print(outputFileCount)
for i in range(int(outputFileCount)):
    fd.append(open(path+str(i), 'w'))

i = 0
with open(traceFile) as csvFile:
    csvReader = csv.reader(csvFile, delimiter=',', skipinitialspace=True)
 
    for row in csvReader:
        if int(float(row[2])/1048576) == 0:
            fd[i%outputFileCount].write("%d, %d, 1, %s, %s, %s, read\n" %(int(row[0]), int(float(row[1])), str(row[3]), str(row[4]), str(row[5])))
        else:
            fd[i%outputFileCount].write("%d, %d, %d, %s, %s, %s, read\n" %(int(row[0]), int(float(row[1])), int(float(row[2])/1048576), str(row[3]), str(row[4]), str(row[5])))
        i = (i+1)%outputFileCount

