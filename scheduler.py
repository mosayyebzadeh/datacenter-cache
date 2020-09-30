from goto import with_goto
import threading, random, itertools
import numpy as np
from collections import deque

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
  def __init__(self, task_id, job, mapper_id, rack, cpu, offset, lenght):
    self.task_id = task_id
    self.job = job
    self.mapper_id = mapper_id
    self.time = 0
    self.rack = rack
    self.offset = offset
    self.lenght = lenght
    self.cpu = cpu
    self.completed = 0

class Job:
  def __init__(self, jid, client, time, objname, mapper, workflowid, size, split_size, iotype):
    self.jid = jid
    self.client = client
    self.submitTime = time  
    self.objname = objname
    self.mapper = mapper  
    self.workflowid = workflowid
    self.size = size
    self.split_size = split_size
    self.iotype = iotype
    self.slot = []


class Scheduler:
  def __init__(self, nodes, cpu, directory, mapper_list, cache_layer):
    self.jobQueue = deque()
    self.nodes = nodes
    self.cpu = cpu
    self.slots = np.zeros((nodes,cpu))
    self.directory = directory
    self.mapper_list = mapper_list
    self.cache_layer = cache_layer
    self.finish = False
    self.rid = Counter()
  
  def addJobs(self, df):
    for i in range(len(df.index)):
      job = Job(i, df.loc[i].user_name, df.loc[i].startTime,  df.loc[i].inputdir,  df.loc[i].mapper,  df.loc[i].workflowid,  df.loc[i].input_size, df.loc[i].mapper_input_size, df.loc[i].iotype)
      self.jobQueue.append(job)
    self.allocateJob()
  
  @with_goto
  def allocateJob(self):
    label .begin
    if self.jobQueue:
      job = self.jobQueue.popleft()
      attrs = vars(job)
      if job.objname in self.directory:
        loc = self.directory[job.objname]
        self.allocateCacheRack(job,loc)
        print('Allocating Cached Data', ', '.join("%s: %s" % item for item in attrs.items()))
        goto .begin
      elif (np.count_nonzero(self.slots == 0) >= job.mapper):
        self.allocateRack(job)
        print('Allocating Random Rack', ', '.join("%s: %s" % item for item in attrs.items()))
        goto .begin
      else:
        self.jobQueue.appendleft(job)
    else:
      self.finish = True

  def allocateRack(self, job):
    free_slots = np.where(self.slots == 0)
    task_id = 0
    for i in range(job.mapper):
      r,c = free_slots[0][i], free_slots[1][i]
      job.slot.append([r,c])
      self.slots[r,c] = 1
      mapper_id  = "map"+str(r)+"-"+str(c)
      offset = i*job.split_size
      task = Task(task_id, job, mapper_id, r, c, offset, offset+job.split_size )
      self.mapper_list[mapper_id].queue.append(task)
      task_id +=1

  def allocateCacheRack(self, job, cache_loc):
    cache_slots = np.where(self.slots[cache_loc] == 0)[0]
    task_id = 0
    for i in cache_slots:
        job.slot.append([cache_loc,i])
        self.slots[cache_loc,i] = 1
        mapper_id  = "map"+str(cache_loc)++"-"+str(i)
        offset = i*job.split_size
        task = Task(task_id, job, mapper_id, cache_loc, i,  offset, offset+job.split_size)
        self.mapper_list[mapper_id].queue.append(task)
        task_id +=1
    remaining = job.mapper - len(cache_slots)
    s = random.sample(range(0, self.cpu), remaining) 
    for i in s:
        job.slot.append([cache_loc,i])
        self.slots[cache_loc,i] = 1
        mapper_id  = "map"+str(cache_loc)++"-"+str(i)
        offset = task_id*split_size
        task = Task(task_id, job, mapper_id, cache_loc, i, offset, offset+job.split_size)
        self.mapper_list[mapper_id].queue.append(task)
        task_id +=1

#'''
#  def start(self):
##    while(not(self.jobQueue.empty()) or nextJob):
#    while(not(self.jobQueue.empty())):
#      print(self.slots)
#      nextJob = self.jobQueue.get()
#      attrs = vars(nextJob)
#      print(', '.join("%s: %s" % item for item in attrs.items()))
#      if (np.count_nonzero(self.slots == 0) >= nextJob.mapper):
#        self.findLocation(nextJob)
#      
#    if self.jobQueue.empty():
#      print("jobqueue is empty")
#      self.finish = True
#
#  def findLocation(self,job):
#    free_slots = np.where(self.slots == 0)
#    if job.objname not in self.directory:
#      for i in range(job.mapper):
#        r,c = free_slots[0][i], free_slots[1][i]
#        job.slot.append([r,c])
#        self.slots[r,c] = 1
#        mapper_id  = "map"+str(r)+"-"+str(c)
#        task = Task(self.task_id, job, mapper_id, r, c)
#        self.mapper_list[mapper_id].queue.append(task) 
#        self.task_id +=1
#
#    else:
#      cache_loc = self.directory[job.objname]
#      cache_slots = np.where(self.slots[cache_loc] == 0)[0]
#      for i in cache_slots:
#        job.slot.append([cache_loc,i])
#        self.slots[cache_loc,i] = 1
#        mapper_id  = str(cache_loc)+"-"+str(i)
#        task = Task(self.task_id, job, mapper_id, cache_loc, i)
#        self.mapper_list[mapper_id].queue.append(task)
#        self.task_id +=1
#      remaining = job.mapper - len(cache_slots)
#      s = random.sample(range(0, self.cpu), remaining)
#      for i in s:
#        job.slot.append([cache_loc,i])
#        self.slots[cache_loc,i] = 1
#        mapper_id  = str(cache_loc)+"-"+str(i)
#        task = Task(self.task_id, job, mapper_id, cache_loc, i)
#        self.mapper_list[mapper_id].queue.append(task)
#        self.task_id +=1
#        '''
#    #print(len(cache_slots))
##    print(self.slots)
#    
#      #np.count_nonzero(self.slots == 0)
# #   print(self.mapper_list.keys())
#
#'''    for i in self.mapper_list.keys():
#      while(not(self.mapper_list[i].queue.empty())):
#        req = self.mapper_list[i].queue.get()
#        attrs = vars(req)
#        print(', '.join("%s: %s" % item for item in attrs.items()))
#        attrs = vars(req.job)
#        print(', '.join("%s: %s" % item for item in attrs.items()))
#'''
