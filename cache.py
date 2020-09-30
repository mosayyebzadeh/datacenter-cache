from lru import LRU

#"""Class representing wb-cache"""
class Cache:

  # Replacement policies
  LRU = "LRU"

  def __init__(self, name, size, policy):
    self.name = name
    self._size = size
    self.free_space = size
    self.policy = policy # Eviction policy
    self.hashmap = {} # Mapping <objname,objsize>

    if (self.policy == Cache.LRU):
      self.cache = LRU(self._size)
    else:
       self.cache = LRU(self._size)

    # Statistics
    self.hit_count = 0
    self.miss_count = 0

  def has_key(self, key):
#    if self.cache.has_key(req.name):
    if key in self.hashmap.keys():
      return True
    else:
      return False

  def update(self,key,size):
    if (self.policy == Cache.LRU):
      self.cache.update(key = size)
      self.hashmap[key] = size
      self.hit_count += 1;    

  def insert(self, key, size):
    while(int(size) > self.free_space):
      self.evict()
    if (self.policy == Cache.LRU):   
      self.cache[key] = size
      self.hashmap[key] = size
      self.free_space += size
      self.miss_count +=1

  def evict(self):
    if (self.policy == Cache.LRU):
      oid = self.cache.peek_last_item()[0]
      del[oid]
      del self.hashmap[oid]     
      self.free_space += int(self.hashmap[oid])

  def put(self, key, size):
    if self.has_key(key):
      self.update(key,size)
    else:
      self.insert(key,size)
      
