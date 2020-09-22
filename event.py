from request import Request
import simpy, datetime, threading, copy

def runMappers(dc, scheduler, env):
  for i in dc.mapper_list.keys():
    request_generator(i, dc, scheduler, env)

def completion(req, dc, env):
  finishTime = env.now
  req.set_endTime(finishTime)
  dc.mapper_list[req.mapper_id].outstanding_req +=1
  print("completion", req.req_id, req.job.jid, req.task.task_id,  dc.mapper_list[req.mapper_id].outstanding_req)  
  if (dc.mapper_list[req.mapper_id].outstanding_req == req.job.split_size/4 ):
    dc.mapper_list[req.mapper_id].outstanding_task.remove(req.task.task_id)
    dc.scheduler.slots[req.task.rack, req.task.cpu] = 0
    dc.mapper_list[req.mapper_id].outstanding_req = 0
    dc.scheduler.allocateJob()
    runMappers(dc, dc.scheduler, env)

def forwardRequest(req, dc):
  objname = req.task.job.objname
  if (dc.placement == "consistent"): 
    dest = dc.consistent_hash(objname)
    if dest in req.path: 
      req.path.append("DL")
    else:
      req.path.append(dest)
      if dc.cache_layer[dest].cache.has_key(objname):
        dc.cache_layer[dest].update(objname = req.task.job.size)
      else:
        dc.cache_layer[dest].miss_count += 1
        req.path.append("DL")    
    
  elif (dc.placement == "directory"):
#    dc.obj_df.loc[objname] = [1,1,'cache3',1,1,1]
    if objname in dc.obj_df.index:
      #hit (L2 or WB)
      dest = dc.obj_df.loc[objname].location
      req.path.append(dest)
      if dc.cache_layer[dest].cache.has_key(objname):
        dc.cache_layer[dest].update(objname = req.task.job.size)
    else:
      #miss
      req.path.append("DL")
    print("forwardRequest", req.req_id, req.task.offset, req.path)



def readReqEvent(req, dc, env):
  yield env.timeout(0)
  objname = req.task.job.objname
  if dc.cache_layer[req.dest].cache.has_key(objname):
    dc.cache_layer[req.dest].cache.update(objname = req.task.job.size)
  else:
    dc.cache_layer[req.dest].miss_count += 1
    forwardRequest(req, dc)
  req.rtype = "readResponse"
  generate_event(req, dc, env)

def readResponseEvent(req, dc, env, links):
    print("readResponseEvent", req.req_id, req.path)
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
      new_req = copy.deepcopy(req)
      del req
      new_req.rtype = "readResponse"
      generate_event(new_req, dc, env)

    else:
      req.path.pop(len(req.path)-1)
      completion(req,dc,env)
#      mapper_id = req.mapper_id
#      request_generator(mapper_id, dc, dc.scheduler, env)   

     
  
def generate_event(req, dc, env):
  if req.rtype == "read":
    req.set_startTime(env.now)
    env.process(readReqEvent(req, dc, env))
  elif req.rtype == "readResponse":
    env.process(readResponseEvent(req, dc, env, dc.links))

def request_generator(mapper_id, dc, scheduler, env):
  q = dc.mapper_list[mapper_id].queue
#  while (scheduler.finish == False):
  if q:
    if not dc.mapper_list[mapper_id].outstanding_task:
      task = q.popleft()
      dc.mapper_list[mapper_id].outstanding_task.append(task.task_id)
      destination = "cache"+str(task.rack)
      source = task.mapper_id
      path = [source,destination]
      chunk_size = 4
      for i in range(task.offset, task.lenght, chunk_size):
        scheduler.rid.increment()
        req_id = scheduler.rid.value()
        req = Request(req_id, mapper_id, task , source, destination, path, i, chunk_size, task.job.iotype)
        attrs = vars(req)
        print('Generating req', ', '.join("%s: %s" % item for item in attrs.items()))
        generate_event(req, dc, env)
  




