from goto import with_goto
import threading, random, itertools
import numpy as np
from collections import deque
import csv, math

class Counter(object):
    def __init__(self):
        self._number_of_read = 0
        self._counter = itertools.count()
        self._read_lock = threading.Lock()

    def increment(self):
        next(self._counter)

    def value(self):
        with self._read_lock:
            value = next(self._counter) - self._number_of_read
            self._number_of_read += 1
        return value

class Task:
  def __init__(self, task_id, job, mapper_id, rack, cpu, offset, lenght, taskReqCount):
    self.task_id = task_id
    self.job = job
    self.mapper_id = mapper_id
    self.time = 0
    self.rack = rack
    self.offset = offset
    self.lenght = lenght
    self.cpu = cpu
    self.taskReqCount = taskReqCount
    self.completed = 0

class Job:
  #def __init__(self, jid, client, time, objname, mapper, workflowid, size, split_size, iotype):
  def __init__(self, jid, time, mapper, size, objname, client, workflowid, iotype, mapper_size, chunk_size):
    self.jid = jid
    self.submitTime = time  
    self.mapper = mapper  
    self.size = size
    self.objname = objname
    self.client = client
    self.workflowid = workflowid
    self.iotype = iotype
    self.mapper_size = mapper_size
    self.chunk_size = chunk_size
    self.taskList = deque()
    self.startTime = 0
    self.endTime = 0
    #self.slot = []


class Scheduler:
  def __init__(self, nodes, cpu, directory, mapper_list, cache_layer, mapper_size, chunk_size):
    self.nodes = nodes
    self.jobQueue = {}
    for i in range(nodes):
        self.jobQueue[i] = deque()
    self.cpu = cpu
    self.slots = np.zeros((nodes,cpu))

    """
    #we consider each rack will have one process running at a same time
    # if you need to run several process, you have to change the following to use CPU
    self.slots = []
    for x in range(nodes):
        self.slots.append(0)
    """

    self.directory = directory
    self.mapper_list = mapper_list
    self.cache_layer = cache_layer
    self.finish = False
    self.rid = Counter()
    #self.jobStat = js 
    self.mapper_size = mapper_size
    self.chunk_size = chunk_size
    #self.delQueue =  deque()
    #self.readQueue =  deque()
    self.startTime = 0 # used for runtime evaluation
    self.endTime = 0 # used for runtime evaluation

  def addJobs(self, rack, traceFile, dataCenter):
    with open(traceFile) as csvFile:
        csvReader = csv.reader(csvFile, delimiter=',', skipinitialspace=True)
        for row in csvReader:
            mapper = math.ceil(float(row[2])/ float(self.mapper_size))
            #print(float(row[2]), str(row[3]))

            job = Job(int(row[0]), int(row[0]), mapper, int(float(row[2])), str(row[3]), str(row[4]), str(row[5]), str(row[6]), self.mapper_size, self.chunk_size)
            #job = Job(int(row[0]), int(row[0]), int(float(row[1])), int(float(row[2])), str(row[3]), str(row[4]), str(row[5]), str(row[6]), 4 * round((int(float(row[2])) / int(float(row[1])))/4))
            self.jobQueue[rack].append(job)
            #self.updateKeySet(str(row[3]), int(row[2]), dataCenter)
        self.allocateJob(rack)

  def updateKeySet(self, objectName, size, dataCenter):
      for i in range(math.ceil(size/dataCenter.chunk_size)):
          dataCenter.setKeys.add(objectName+"_"+str(i))

  @with_goto
  def allocateJob(self, rack):
    # wait for write/read finished before delete it
    label .begin
    if self.jobQueue[rack]:
      job = self.jobQueue[rack].popleft()
      attrs = vars(job)
      #print("creating task for: ", job.__dict__)
      #FIXME: AMIN: if we want to simulate schedular working with the directory
      # we need to un-comment this section and add our code here.
      """ 
      if job.objname+"_0" in self.directory.df.index:
        loc = self.directory.df.loc[job.objname+'_0', 'location']
        cache_loc = int(loc[0].split("cache")[1])
        self.allocateCacheRack(job,cache_loc)
        goto .begin
      elif (np.count_nonzero(self.slots == 0) >= job.mapper):
      """
      if (np.count_nonzero(self.slots[rack] == 0) > 0):
      #if self.slots[rack] == 0:

        self.createTask(rack, job)
        self.allocateRack(rack, job)
        goto .begin
      else:
        #print("no job allocation  ")
        self.jobQueue[rack].appendleft(job)
    else:
      #print("finish is True")
      self.finish = True
    
  """
  def inProcess(self, job):
    if job.iotype == 'delete':
      job.split_size = 4
      return self.jobStat.inProgress(job.objname)
    else:
      return False

  def inProcessWrite(self, job):
    if job.iotype == 'read':
      return self.jobStat.inProgressWrite(job.objname)
    else:
      return False

  """

  def createTask(self, rack, job):
    #print("rack is %s" %rack)
    task_id = 0
    lenght = job.size
    #print("job:", job.__dict__)
    for i in range(job.mapper):
        #print("i is %s" %i)
        offset = i*job.mapper_size
        task_size = job.mapper_size
        if( lenght < job.mapper_size):
            task_size = lenght;  
        taskReqCount = math.ceil(task_size/job.mapper_size)
        #print("taskReqCount is: ", taskReqCount)
        task = Task(task_id, job, "", rack, 0, offset,  task_size, taskReqCount)
        lenght -= job.mapper_size
        task_id +=1
        job.taskList.append(task)
        #print("Create TASK ", task.__dict__)
 
  def allocateRack(self, rack, job):
    #print("rack is %s" %rack)

    """
    if self.slots[rack] == 1:
        return
    """
    free_slots = np.where(self.slots[rack] == 0)
    if len(free_slots[0]) == 0:
        return

    for i in free_slots[0]:
        if len(job.taskList) > 0:
            task = job.taskList.popleft()
            #job.slot.append([rack,i])
            self.slots[rack][i] = 1
            task.mapper_id  = "map"+str(rack)+"-"+str(i)
            task.cpu = i
            #print("Allocate RACK: ", task.__dict__)
            self.mapper_list[task.mapper_id].queue.append(task)

