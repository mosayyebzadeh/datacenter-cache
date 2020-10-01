from uhashring import HashRing
from cache import Cache
from directory import Directory
import simpy, re, random, threading
import pandas as pd
from collections import deque
from stats import JobStat

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
    self.lock = threading.Lock()
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
    self.blk_dir = Directory('blk_dir') #block directory
    self.jobStat = JobStat()
    self.hash_ring = None
    self.nic_count = 0
    self.scheduler = None
    self.repType = 'rep'
    self.repCount = 3
    self.ec=[]
    self.rep_size = 4
    self.chunk_size = 4
    self.osdMap = None  
    self.dl_access = 0 
  def build_directory(self):
    print('Building Datacenter with block and object directory')
    col_names = ['blkname', 'c_time', 'size', 'location', 'owner', 'freq', 'la_time']
    self.blk_dir.df = pd.DataFrame(columns = col_names)
    self.blk_dir.df = self.blk_dir.df.set_index(['blkname'])
    
    col_names = ['objname', 'c_time', 'size', 'location', 'owner', 'freq', 'la_time']
    self.blk_dir.obj_df = pd.DataFrame(columns = col_names)
    self.blk_dir.obj_df = self.blk_dir.obj_df.set_index(['objname'])

    print('Building osd mapping for write cache')
    col_names = ['blkname', 'osd_list', 'dirty']
    self.osdMap = pd.DataFrame(columns = col_names)
    self.osdMap = self.osdMap.set_index(['blkname'])

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
    self.chunk_size = int(config.get('Simulation', 'chunk size')) # in MB
    self.logger = logger
    if (self.placement == "consistent"):
      self.hash_ring = consistentHashing(self.c_nodes)
    elif (self.placement == "directory"):
      self.build_directory()
#      for i in range(4):
#        self.obj_directory["file"+str(i)] = ["cache12"]
    self.repType = config.get('Simulation', 'replication type')
    if self.repType == 'rep':
      self.repCount = int(config.get('Simulation', 'replication count'))
      self.rep_size = self.chunk_size
    elif self.repType == 'ec':
      tmp = config.get('Simulation', 'replication count').split(',')   
      self.ec = [int(tmp[0]),int(tmp[1])]
      self.rep_size = float (self.chunk_size / self.ec[0])
    print("reptyep","repcount",self.repType, self.rep_size)
    print ("Building Datacenter with ", self.placement)
    logger.info('Building Datacenter with')

    for i in range(self.c_nodes):
      c_name = "cache"+str(i) #i is rack id
      self.cache_layer[c_name]=Cache(c_name, size, policy)
    
    c_name = 'writeCache'
    self.cache_layer[c_name]=Cache(c_name, size, 'FIFO')

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
  
  def get_replica_loc(self, cache_id):
    candidates = []
    for i in range(self.c_nodes):
     candidates.append('cache'+str(i))
    candidates.remove(cache_id)
    if self.repType == 'rep':
      return  random.sample(candidates,  self.repCount-1)
    elif self.repType == 'ec':
      count = int(self.ec[0])+int(self.ec[1])
      return random.sample(candidates,  count-1)
    
  def datalake_access(self):
    self.dl_access += 1



