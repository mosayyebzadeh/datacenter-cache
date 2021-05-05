import sys
import csv


traceFile = str(sys.argv[1])
path = str(sys.argv[2])

startLine  = int(sys.argv[3])
lineCount  = int(sys.argv[4])
#traceFile = '/home/centos/IBMTraces/trace003GET.csv'
#path = '/home/centos/datacenter-cache-multi-sim2/trace003GET.csv'
with open(traceFile) as csvFile:
    with open(path, 'w') as fd:
        csvReader = csv.reader(csvFile, delimiter=' ', skipinitialspace=True)
        for i in range(startLine):
            next(csvReader)
        for i in range(lineCount):
            row = next(csvReader)

            if int(float(row[2])/1048576) == 0:
                fd.write("%d, %d, 1, %s, %s, %s, read\n" %(int(row[0]), int(float(row[1])), str(row[3]), str(row[4]), str(row[5])))
            else:
                fd.write("%d, %d, %d, %s, %s, %s, read\n" %(int(row[0]), int(float(row[1])), int(float(row[2])/1048576), str(row[3]), str(row[4]), str(row[5])))

