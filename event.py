from request import Request
import simpy, datetime, threading, copy
from cache import *
from osd_op import *

def runMappers(dc, scheduler, env):
  for i in dc.mapper_list.keys():
    request_generator(i, dc, scheduler, env)

def completion(req, dc, env):
  finishTime = env.now
  req.set_endTime(finishTime)
  dc.mapper_list[req.mapper_id].outstanding_req +=1
#  print("Req completion", req.req_id, req.name)  
  if (dc.mapper_list[req.mapper_id].outstanding_req == req.job.split_size/4 ):
    dc.mapper_list[req.mapper_id].outstanding_task.remove(req.task.task_id)
    dc.scheduler.slots[req.task.rack, req.task.cpu] = 0
    dc.mapper_list[req.mapper_id].outstanding_req = 0
    dc.lock.acquire()
    print("Task completion", req.job.objname, req.task.task_id, req.task.mapper_id)  
    dc.jobStat.update(req.job.jid, finishTime)
    if dc.jobStat.isFinished(req.job.jid):
      dc.blk_dir.insertObj(req.job.objname, req.job.size, req.job.client,finishTime)
    dc.lock.release()
    dc.scheduler.allocateJob()
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
        dc.cache_layer[dest].put(req.name, req.size)
      else:
        req.path.append("DL")    
        dc.datalake_access()

  elif (dc.placement == "directory"):
    if dc.blk_dir.haskey(req.name): # Cache Hit
      dest = dc.blk_dir.get_location(req.name)
      req.path.append(dest)
      if req.name in dc.cache_layer[dest].hashmap.keys():
        dc.cache_layer[dest].put(req.name, req.size)
        dc.blk_dir.put(req.name, dc.cache_layer[dest], env.now)
    else: #Cache miss
       req.path.append("DL")
       dc.datalake_access()

def readReqEvent(req, dc, env):
  yield env.timeout(0)
  if dc.cache_layer[req.dest].has_key(req.name): # Local Cache Hit
    dc.cache_layer[req.dest].put(req.name, req.size)
    dc.blk_dir.put(req.name, req, req.dest, env.now)
  else:
    forwardRequest(req, dc, env) 
  generate_event(req, dc, env, 'readResponse')

def readResponseEvent(req, dc, env, links):
    source, dest = req.path[-1], req.path[-2]
    req.path.pop(len(req.path)-1) 
    sLink, dLink = dc.get_link_id(source, dest)
    # Get the required amount of Bandwidth
    latency = 0
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
      if req.name not in dc.cache_layer[dest].hashmap.keys():
        dc.blk_dir.put(req.name, req, dest, env.now)
      generate_event(req, dc, env, "readResponse")
      
    else:
      req.path.pop(len(req.path)-1)
      completion(req,dc,env)

def writeReqEvent(req, dc, env, links):
  sLink, dLink = dc.get_link_id(req.path[-2], req.path[-1])
  latency = 0
  if sLink:
    yield links[sLink].get(links[sLink].capacity)
    latency = float(req.job.size) / links[sLink].capacity
    yield env.timeout(latency)
    yield links[sLink].put(links[sLink].capacity)
  
  candidates = dc.get_replica_loc(req.dest)
  for i in candidates:
    sLink, dLink = dc.get_link_id(req.dest, i)
    yield links[dLink].get(links[dLink].capacity)
  latency = float(dc.rep_size) / links[dLink].capacity
  yield env.timeout(latency)

  for i in candidates:
    sLink, dLink = dc.get_link_id(req.dest, i)
    yield links[dLink].put(links[dLink].capacity)

  # write object and replicate to osd map, insert data to wb-cache and update directory
  insert_osd_map(dc.osdMap, req.name, candidates, True) 
  dc.cache_layer['writeCache'].put(req.name, dc.rep_size*(len(candidates)+1))
  dc.blk_dir.put(req.name, req, 'writeCache', env.now)
  completion(req,dc,env)
  flushEvent(dc, env, links)

def flushEvent(dc, env, links):
  candidates = dc.blk_dir.aged_items(1, 3)  
  print('flush',candidates) 

def generate_event(req_old, dc, env, event_type):
  req = copy.deepcopy(req_old)
  req.rtype = event_type
  if req.rtype == "read":
    req.set_startTime(env.now)
    env.process(readReqEvent(req, dc, env))
  elif req.rtype == "readResponse":
    env.process(readResponseEvent(req, dc, env, dc.links))
  elif req.rtype == "write":
    req.set_startTime(env.now)
    env.process(writeReqEvent(req, dc, env, dc.links))

def request_generator(mapper_id, dc, scheduler, env):
  q = dc.mapper_list[mapper_id].queue
  if q:
    if not dc.mapper_list[mapper_id].outstanding_task:
      task = q.popleft()
      dc.lock.acquire()
      if not (task.job.jid in dc.jobStat.df.index):
        dc.jobStat.insert(task.job.jid, task.job, env.now) # for job stats
      dc.lock.release()
      dc.mapper_list[mapper_id].outstanding_task.append(task.task_id)
      destination = "cache"+str(task.rack)
      source = task.mapper_id
      path = [source,destination]
      for i in range(task.offset, task.lenght, dc.chunk_size):
        scheduler.rid.increment()
        req_id = scheduler.rid.value()
        req = Request(req_id, mapper_id, task , source, destination, path, i, dc.chunk_size, task.job.iotype)
        print("Request:", req.req_id, req.name, req.task.job.iotype)  
        generate_event(req, dc, env, task.job.iotype)




