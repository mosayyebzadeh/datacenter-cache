from lru import LRU

#"""Class representing wb-cache"""
class Cache:

  # Replacement policies
  LRU = "LRU"

  def __init__(self, name, size, policy):
    self.name = name
    self._size = size
    self._free_space = size
    self.policy = policy # Eviction policy
    self.hashmap = {} # Mapping <objname,objsize>

    if (self.policy == Cache.LRU):
      self.cache = LRU(self._size)
    else:
       self.cache = LRU(self._size)

    # Statistics
    self.hit_count = 0
    self.miss_count = 0
    self.backend_bw = 0
    self.crossrack_bw = 0
    self.intrarack_bw = 0

  def update(self,key,size):
    self.cache.update(key = size)
    self.hit_count += 1;    

  def insert(self, key, size):
    while(int(size) > self._free_space):
      self.evict()
    if (self.policy == Cache.LRU):   
      self.cache[key] = size
      self.hashmap[key] = size
      self._free_space += size

  def evict(self):
    if (self.policy == Cache.LRU):
      oid = self.cache.peek_last_item()[0]
      del[oid]
      del self.hashmap[oid]     
      self._free_space += int(self.hashmap[oid])

class UserDirectory:
  def __init__(self):
    self.a = 1



class ObjDirectory:
  def __init__(self):
    self.a = 1


class Object:
  def __init__(self, owner, bucket, objname, c_time, a_time, freq, classid, location):
    self.owner = owner
    self.bucket = bucket
    self.objname = objname
    self.c_time = c_time
    self.freq = freq
    self.classid = self.classid
    self.location = location

class User:
  def __init__(self, userid):  
    self.userid = userid
    self.deletion = 0
    self.re_access = 0
    self.written = 0
    self.avg_reaccess_rate = 0
    self.avg_delete_rate = 0

