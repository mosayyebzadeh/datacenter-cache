from uhashring import HashRing
from cache import Cache
import simpy, re
import pandas as pd
from collections import deque

class Mapper:
  def __init__(self, name):
    self.name = name
    self.queue = deque()
    self.outstanding_req = 0
    self.outstanding_task = []
    

def setUp(hashType,nodeNum):
  nodes={}
  if (hashType == "consistent"):
    for i in range(int(nodeNum)):
      cname='cache'+str(i)
      nodes[cname]={'hostname':cname, 'weight': 1}
  return nodes

def consistentHashing(nodeNum):
   nodes=setUp("consistent",nodeNum)
   return HashRing(nodes,vnodes=200,hash_fn='ketama')

class DataCenter:
  def __init__(self, name):
    self.name = name
    self.config = None
    self.cache_layer = {}
    self.c_nodes = 0
    self.links = {}
    self.env = None
    self.compute_nodes = 0    
    self.mapper_list = {}
    self.cpu = 0    
    self.placement = None 
    self.logger = None
    self.obj_df = None #obj directory
    self.hash_ring = None
    self.nic_count = 0
    self.scheduler = None
  def build_directory(self):
    print('Building Datacenter with dir')
    col_names = ['objname', 'c_time', 'size', 'location', 'owner', 'freq', 'la_time']
    self.obj_df = pd.DataFrame(columns = col_names)
    self.obj_df = self.obj_df.set_index(['objname'])
    print(self.obj_df)

  def build_worker_nodes(self):
   for r in range(self.compute_nodes):
    for c in range(self.cpu):
      name = "map"+str(r)+"-"+str(c)
      mapper = Mapper(name)
      self.mapper_list[name] = mapper

  def build_network(self, logger,env):
    # nic0 -> client to cache server
    # nic1 -> cross rach(between cache servers
    # nic2 -> between backend and cache servers

    links={}
    nic_count =  int(self.config.get('Network', 'nic count'))
    self.nic_count = nic_count
    for i in range(self.c_nodes):
      for j in range (nic_count-1):
        nic_id = "nic"+str(j)+" in"
        link_id = "nic"+str(j)+".in."+str(i)
        speed = float(self.config.get('Network', nic_id).split("G")[0])/8*1000*1000*1000
        links[link_id]=simpy.Container(env, speed, init=speed)
        
        nic_id = "nic"+str(j)+" out"
        link_id = "nic"+str(j)+".out."+str(i)
        speed = float(self.config.get('Network',nic_id).split("G")[0])/8*1000*1000*1000
        links[link_id]=simpy.Container(env, speed, init=speed)
    
    #DL link
    nic_id = "nic"+str(nic_count-1)+" in"
    link_id = "nic"+str(nic_count-1)+".in" 
    speed = float(self.config.get('Network', nic_id).split("G")[0])/8*1000*1000*1000 
    links[link_id]=simpy.Container(env, speed, init=speed)
    
    nic_id = "nic"+str(nic_count-1)+" out"
    link_id = "nic"+str(nic_count-1)+".out" 
    speed = float(self.config.get('Network', nic_id).split("G")[0])/8*1000*1000*1000 
    links[link_id]=simpy.Container(env, speed, init=speed)
  
    return links
  
  def build(self, config, logger, env):
    self.config = config
    self.c_nodes = int(config.get('Simulation', 'cache nodes'))
    self.placement = config.get('Simulation', 'placement')
    policy = config.get('Simulation', 'cache policy')
    size = int(config.get('Simulation', 'cache capacity')) #in bytes
    self.compute_nodes = int(config.get('Simulation', 'compute nodes')) 
    self.cpu = int(config.get('Simulation', 'cpu'))
    self.logger = logger
    if (self.placement == "consistent"):
      self.hash_ring = consistentHashing(self.c_nodes)
    elif (self.placement == "directory"):
      self.build_directory()
#      for i in range(4):
#        self.obj_directory["file"+str(i)] = ["cache12"]
    print ("Building Datacenter with ", self.placement)
    logger.info('Building Datacenter with')

    for i in range(self.c_nodes):
      c_name = "cache"+str(i) #i is rack id
      self.cache_layer[c_name]=Cache(c_name, size, policy)
    self.links = self.build_network(logger,env)
     
    print ("Building compute nodes and mappers")
    logger.info('Building compute nodes and mappers')
    self.build_worker_nodes()

    print ("Building scheduler")
    logger.info('Building scheduler')
    


  def consistent_hash(self,key):
      return self.hash_ring.get_node(key)

  def get_link_id(self, source, dest):
    if source == "DL":
      s =  "nic"+str(self.nic_count-1)+".in"
      return s, None
    elif "map" in source:
      result = re.search('map(.*)-', source)
      rack = result.group(1)
      s = "nic0.in."+str(rack)
      return s, None
    else:
      rack = source.split("cache",1)[1]
      s = "nic1.out."+str(rack)
 
    if dest == "DL":
      d = "nic"+str(self.nic_count-1)+".out"
      return None, d
    elif "map" in dest:
      result = re.search('map(.*)-', dest)
      rack = result.group(1)
      d = "nic0.out."+str(rack)
      return None, d
    else:
      rack = dest.split("cache",1)[1]
      d = "nic1.in."+str(rack)
    return s, d

