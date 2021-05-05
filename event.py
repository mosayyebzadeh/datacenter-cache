from request import Request
import simpy, datetime, threading, copy, math
from cache import *
from osd_op import *
from timer import Timer


def printCacheStats(dc):
    print("-------Cache-------")
    d = dc.blk_dir.dict
    for k in dc.cache_layer.keys():
        print(dc.cache_layer[k].name, list(dc.cache_layer[k].cache.keys()) )
    for k in d.keys():
        print(k, d[k]['gfreq'], d[k]['location'], d[k]['valid'])

def runMappers(dc, scheduler, env):
  for i in dc.mapper_list.keys():
    request_generator(i, dc, scheduler, env)

def completion(req_old, dc, env):
  #print("%s: Completion event is %s, path is %s" %(env.now, req_old.job.objname, req_old.path))
  #req = copy.deepcopy(req_old)
  req = req_old
  finishTime = env.now
  req.set_endTime(finishTime)
  dc.mapper_list[req.mapper_id].outstanding_req +=1
  #print("outstanding req is: ", dc.mapper_list[req.mapper_id].outstanding_req)
  #print("taskReqCount is: ", req.task.taskReqCount)
  dc.outstanding_req[req.req_id] = [False, req.job.jid] 
  #if (dc.mapper_list[req.mapper_id].outstanding_req == req.job.split_size/dc.chunk_size ): # task is done, release mapper
  #if (dc.mapper_list[req.mapper_id].outstanding_req == req.task.reqCount ): # task is done, release mapper
  if (dc.mapper_list[req.mapper_id].outstanding_req == req.task.taskReqCount ): # task is done, release mapper
    #printCacheStats(dc)
    dc.mapper_list[req.mapper_id].outstanding_task.remove(req.task.task_id)
    dc.scheduler.slots[req.task.rack] = 0
    dc.mapper_list[req.mapper_id].outstanding_req = 0
    #dc.lock.acquire()
    #dc.jobStat.update(req.job.jid, finishTime) # update job finish time
    #if dc.jobStat.isFinished(req.job.jid) and req.rtype == 'write': # finish write request
    #  dc.cache_layer['writeCache'].put(req.job.objname, req.job.size, dc)
    #dc.lock.release()
    dc.cache_layer["cache"+str(req.task.rack)].taskCount += 1
    if dc.cache_layer["cache"+str(req.task.rack)].policy == "LORE":
        if dc.cache_layer["cache"+str(req.task.rack)].taskCount >= dc.cache_layer["cache"+str(req.task.rack)].size/2:
          agingFunc(dc)
    if len(req.job.taskList) > 0: 
        #print("there are still tasks", req.job.taskList) 
        dc.scheduler.allocateRack(req.task.rack, req.job)
    else:
        #for rack in range(dc.c_nodes):
        dc.jobDoneCount += 1
        
        if dc.jobDoneCount % 1000 == 0:
            #dc.timer.stop()
            dc.printCount += 1
            print("%d jobs are COMPLETE. This is %d times"  %(dc.jobDoneCount, dc.printCount))
            #dc.timer.start()
        dc.scheduler.allocateJob(req.task.rack)
    #print("We ARE Here", req.job.taskList)
    runMappers(dc, dc.scheduler, env)
  
def forwardRequest(req, dc, env):
  if (dc.placement == "consistent"): 
    dest = dc.consistent_hash(req.name)
    if dest in req.path: 
      req.path.append("DL")
      dc.datalake_access()
    else:
      req.path.append(dest)
      if dc.cache_layer[dest].cache.has_key(req.name):
        dc.cache_layer[dest].put(req.name, req.size, dc)
      else:
        req.path.append("DL")    
        dc.datalake_access()

  elif (dc.placement == "directory"):
    # Remote cache HIT
    if dc.blk_dir.haskey(req.name) and dc.blk_dir.dict[req.name]['valid'] == 1:
      #print("forward Event1:", req.name, req.path)  
      dest = dc.blk_dir.get_location(req.name) #
      req.path.append(dest)
      if dc.cache_layer[dest].has_key(req.name):
        dc.cache_layer[dest].put(req.name, req.size, dc)
        dc.cache_layer[dest].remote_hit +=1
        dc.blk_dir.updateGFreq(req.name)
      else: #Cache miss
        #print("%s: forward req1, path is %s" %(req.name, req.path))
        req.path.append("DL")
        #print("%s: forward req2, path is %s" %(req.name, req.path))
        dc.cache_layer[req.dest].miss_count +=1
        if (req.name in dc.blk_dir.dict):
          dc.blk_dir.updateGFreq(req.name)
        dc.datalake_access()
    else: #Cache miss
       #print("%s: forward req3, path is %s" %(req.name, req.path))
       req.path.append("DL")
       #print("%s: forward req4, path is %s" %(req.name, req.path))
       dc.cache_layer[req.dest].miss_count +=1
       if (req.name in dc.blk_dir.dict):
         dc.blk_dir.updateGFreq(req.name)
       dc.datalake_access()


def readReqEvent(req, dc, env):
  #print("Read event")
  #eventTimer = Timer()
  #eventTimer.start()
  yield env.timeout(0)
  #eventTimer.stop()
  #print("Read Event:", req.name, req.path)  
  #printCacheStats(dc)
  if dc.cache_layer[req.dest].has_key(req.name): # Local Cache Hit
    dc.cache_layer[req.dest].put(req.name, req.size, dc)
    dc.cache_layer[req.dest].local_hit +=1
    dc.blk_dir.updateGFreq(req.name)
    #dc.blk_dir.put(req.name, req.size, req.dest, env.now)
  else:
    forwardRequest(req, dc, env) 
  #readResponse(req, dc, env, dc.links)
  #print("%s: Read event path is %s" %(req.name, req.path))
  generate_event(req, dc, env, 'readResponse')

def readResponseEvent(req, dc, env, links):
    #yield env.timeout(0)
    source, dest = req.path[-1], req.path[-2]
    req.path.pop(len(req.path)-1) 
    # Get the required amount of Bandwidth
    latency = 0

    sLink, dLink = dc.get_link_id(source, dest)
    if sLink:
      yield links[sLink].get(links[sLink].capacity)
      latency = float(req.job.size) / links[sLink].capacity
    if dLink:
      yield links[dLink].get(links[dLink].capacity)
      latency = max(float(req.job.size) / links[dLink].capacity, latency)
    
    yield env.timeout(latency)
    # Put the required amount of Bandwidth
    if sLink:
      yield links[sLink].put(links[sLink].capacity)
    if dLink:
      yield links[dLink].put(links[dLink].capacity)
    
    if (len(req.path) >= 2):
      #print("PATH is longer than 2", req.path)
#      if req.name not in dc.cache_layer[dest].hashmap.keys():
      dc.cache_layer[req.dest].put(req.name, req.size, dc)
      #dc.blk_dir.put(req.name, req.size, dest, env.now)
      #readResponse(req, dc, env, dc.links)
      #print("%s: ReadResponse event 2 is %s, path is %s" %(req.name, req.job.objname, req.path))
      generate_event(req, dc, env, "readResponse")
      #print("%s: ReadResponse event 3 is %s, path is %s" %(req.name, req.job.objname, req.path))
      
    else:
      #print("calling completion")
      req.path.pop(len(req.path)-1)
      completion(req,dc,env)



def agingFunc(dc):
  
  #print('AMIIIIIIIIN AGING EVENT inerval is %s' %env.now)
  for i in range(dc.c_nodes):
    c_name = "cache"+str(i) #i is rack id
    dc.cache_layer[c_name].halve_lfreq()
    #print("TASK COUNT of %s is %d" %(c_name, dc.cache_layer[c_name].taskCount))
    dc.cache_layer[c_name].taskCount = 0
  dc.blk_dir.halve_gfreq()
  #yield env.timeout(interval)
  #env.process(agingEvent(dc, env, interval))
  
def cleanUpDir(dc, env, interval):
  env.process(cleanUpEvent(dc, env, interval))

def cleanUpEvent(dc, env, interval):
  #print('AMIIIIIIIIN CLEANUP EVENT inerval is %s' %env.now)
  dc.blk_dir.removeEntry()
  yield env.timeout(interval)
  env.process(cleanUpEvent(dc, env, interval))
    




def generate_event(req_old, dc, env, event_type):
  #req = copy.deepcopy(req_old)
  #del req_old
  req = req_old
  req.rtype = event_type
  req.set_startTime(env.now)
  #print("Generate event:", req.name, req.rtype, req.path)  
  if req.rtype == "read":
    event = env.process(readReqEvent(req, dc, env))
  elif req.rtype == "readResponse":
    #print("%s: Generated RESPONSE event is %s, path is %s" %(env.now, req.job.objname, req.path))
    env.process(readResponseEvent(req, dc, env, dc.links))

def request_generator(mapper_id, dc, scheduler, env):
  q = dc.mapper_list[mapper_id].queue
  if q:
    if not dc.mapper_list[mapper_id].outstanding_task:
      task = q.popleft()
      #dc.lock.acquire()
      #if not (task.job.jid in dc.jobStat.df.index):
      #  dc.jobStat.insert(task.job.jid, task.job, env.now) # for job stats
      dc.mapper_list[mapper_id].outstanding_task.append(task.task_id)
      #dc.lock.release()
#      print("testing", task.offset, task.offset + task.lenght,  task.__dict__)
      for i in range(task.offset, task.offset + task.lenght, dc.chunk_size):
        destination = "cache"+str(task.rack)
        source = task.mapper_id
        path = [source,destination]
        req_offset = i
        req_size = dc.chunk_size
        scheduler.rid.increment()
        req_id = scheduler.rid.value()
        #dc.lock.acquire()
        dc.outstanding_req[req_id] = [True, task.job.jid]
        #dc.lock.release()
        req = Request(req_id, mapper_id, task , source, destination, path, req_offset, req_size, task.job.iotype)
        #task.reqCount += 1
        
        #print("Generator Request:", req.req_id, req.name, req.task.job.iotype, req.offset, req.end, req.dest, mapper_id, req.path)  
        #print("Generator Request:", req.rtype)  
        #readReq(req, dc, env)
        generate_event(req, dc, env, task.job.iotype)
        #print("req id:", req_id, "job id:", req.task.job.jid, "objname:", req.task.job.objname, "mapper:", req.task.job.mapper, "rack:", req.task.rack, "cpu:", req.task.cpu, "task offset:", req.task.offset)
        if task.job.iotype == 'delete':
          break;

