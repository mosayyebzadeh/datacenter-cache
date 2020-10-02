from lru import LRU
import queue
#"""Class representing wb-cache"""
class Cache:

  # Replacement policies
  LRU = "LRU"
  FIFO = 'FIFO'

  def __init__(self, name, size, policy):
    self.name = name
    self.size = size
    self.free_space = size
    self.policy = policy # Eviction policy
    self.hashmap = {} # Mapping <objname,objsize>

    if (self.policy == Cache.LRU):
      self.cache = LRU(self.size)
    elif (self.policy == Cache.FIFO):
       self.cache = queue.Queue(maxsize=self.size)

    # Statistics
    self.hit_count = 0
    self.miss_count = 0

  def has_key(self, key):
    if key in self.hashmap.keys():
      return True
    else:
      return False

  def update(self,key,size):
    self.hashmap[key] = size
    self.hit_count += 1;    
    if (self.policy == Cache.LRU):
      self.cache.update(key = size)
    elif (self.policy == Cache.FIFO):
      self.cache.put(key)

  def insert(self, key, size, directory):
    if (self.policy == Cache.LRU):
      self.insertLRU(key, size, directory)
    elif (self.policy == Cache.FIFO):
      self.insertFIFO(key, size, directory)

  def evictLRU(self, directory):
    oid = self.cache.peek_last_item()[0]
    directory.removeBlock(oid, self.name)
    del[oid]
    del self.hashmap[oid]     
    self.free_space += int(self.hashmap[oid])

  def evictFIFO(self, directory):
    oid = self.cache.get()
    directory.removeBlock(oid, self.name)
    self.free_space += int(self.hashmap[oid])
    del self.hashmap[oid]     
  
  def insertLRU(self, key, size, directory):
    while(int(size) >= self.free_space):
      self.evictLRU(directory)
    self.cache[key] = size
    self.hashmap[key] = size
    self.free_space += size
    self.miss_count +=1

  def insertFIFO(self, key, size, directory):
    while(int(size) >= self.free_space):
      self.evictFIFO(directory)
    self.cache.put(key)
    self.hashmap[key] = size
    self.free_space += size
    self.miss_count +=1

  def put(self, key, size, directory):
    if self.has_key(key):
      self.update(key,size)
    else:  
      self.insert(key, size, directory)


  def print(self):
    if (self.policy == Cache.LRU):
      print(self.name, "LRU", self.hashmap, self.cache.items())
    elif (self.policy == Cache.FIFO):
      print(self.name, "LRU", self.hashmap, list(self.cache.queue))

  def remove(self,key):
    del self.hashmap[key]
    if (self.policy == Cache.LRU):
      del self.cache[key]
    elif (self.policy == Cache.FIFO):
      a=4 #FIXME    
