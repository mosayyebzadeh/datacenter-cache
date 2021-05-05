from lru import LRU
import queue
from timer import Timer

class Cache:

  # Replacement policies
  LRU = "LRU"
  FIFO = 'FIFO'
  LORE = 'LORE'
  LFUL = 'LFUL'
  LFUG = 'LFUG'

  def __init__(self, name, size, policy, interval, setKeys):
    self.name = name
    self.size = size
    self.interval = interval
    self.free_space = size
    self.policy = policy # Eviction policy
    self.taskCount = 0
    self.cacheTimer = Timer()

    if (self.policy == Cache.LRU):
      self.cache = LRU(self.size)
    elif (self.policy == Cache.LFUL):
      self.cache = {}
    elif (self.policy == Cache.LFUG):
      self.cache = {}
    elif (self.policy == Cache.LORE):
      value = {"size": 0, "lfreq":0, "location":set()}
      self.cache = dict.fromkeys(setKeys, value)
      self.cache.update(dict(self.cache))
    elif (self.policy == Cache.FIFO):
       self.cache = queue.Queue(maxsize=self.size)

    # Statistics
    self.hit_count = 0
    self.miss_count = 0
    self.local_hit = 0
    self.local_miss = 0
    self.remote_hit = 0
    self.remote_miss = 0

  def has_key(self, key):
    if key in self.cache:
      return True
    else:
      return False

  def put(self, key, size, dataCenter):
    #if self.has_key(key):
    #print("cache put")
    #self.cacheTimer.start()
    if key in self.cache:
      self.update(key,size, dataCenter)
    else:  
      self.insert(key, size, dataCenter)
    #self.cacheTimer.stop()
    """
    print("*********************")
    self.print()
    print("DIRECTORY:", directory.dict)
    print("#####################")
    """

  def update(self,key,size, dc):
    self.hit_count += 1;    
    if (self.policy == Cache.LRU):
        value = self.cache[key]
        #print("key is ", key, "LRU VALUE before update", self.cache[key])
        #locations = self.cache[key].["location"]
        #locations.append(self.name)
        #value = {"size": size, "lfreq":1, "location":locations, "time":0}
        #value = {"size": size,"location":locations}
        #self.cache[key]["location"].add(self.name)
        #print("key is ", key, "LRU VALUE after update" , self.cache[key])

    if (self.policy == Cache.LFUL):
        #value = self.cache[key]
        #lfreq = value["lfreq"] + 1
        #locations = value["location"]
        #if self.name not in locations:
        #    locations.append(self.name)
        #value = {"size": size, "lfreq":lfreq, "location":locations, "time":time}
        self.cache[key]["lfreq"] += 1

    elif (self.policy == Cache.LFUG):
        dc.blk_dir.put(key, size, self.name)

    elif (self.policy == Cache.LORE):
        value = self.cache[key]
        value["size"] = size
        value["location"].add(self.name)
        value["lfreq"] += 1
        self.cache[key] = value


    elif (self.policy == Cache.FIFO):
        self.cache.put(key)

  def insert(self, key, size, dc):
    if (self.policy == Cache.LRU):
      self.insertLRU(key, size)
    elif (self.policy == Cache.LFUL):
      self.insertLFUL(key, size)
    elif (self.policy == Cache.LFUG):
      self.insertLFUG(key, size)
    elif (self.policy == Cache.LORE):
      self.insertLORE(key, size, dc)
    elif (self.policy == Cache.FIFO):
      self.insertFIFO(key, size)

 
  def insertLRU(self, key, size):
    while(int(size) > self.free_space):
      self.evictLRU()
    #lfreq = 1
    #value = {"size": size, "location":{self.name}}
    self.cache[key] = size
    self.free_space -= size

  def insertLFUL(self, key, size):
    while(int(size) > self.free_space):
        self.evictLFUL()
    value = {"size": size, "lfreq": 1}
    self.cache[key] = value
    self.free_space -= size

  def insertLFUG(self, key, size):
    while(int(size) > self.free_space):
        self.evictLFUG()
    value = {"size": size, "location":{self.name}}
    self.cache.update({key: value})
    directory.put(key, size, self.name)
    self.free_space -= size

  #value is a dictionary (size, lfreq, location)
  def insertLORE(self, key, size, dc):
    while(int(size) > self.free_space):
        if not self.evictLORE(key, dc):
            return
    value = {"size": size, "lfreq":1, "location":{self.name}}
    self.cache.update({key: value})
    dc.blk_dir.put(key, size, self.name)
    self.free_space -= size

  def insertFIFO(self, key, size):
    while(int(size) >= self.free_space):
        self.evictFIFO()
    self.cache.put(key)
    self.free_space -= size

  def evictLRU(self):
    oid = self.cache.peek_last_item()[0]
    self.free_space += int(self.cache[oid])
    del self.cache[oid]

  def evictLFUL(self):
    keyMin = min(self.cache, key= lambda x: self.cache[x]['lfreq'])
    self.free_space += int(self.cache[keyMin]['size'])
    del self.cache[keyMin]

  #FIXME: we need to make sure the found key is present in the local cache
  def evictLFUG(self):
      #if Keymin = min(directory, key= lambda x: directory[x]['gfreq']) in self.cache.keys():
        self.free_space += int(self.cache[oid]['size'])
        del self.cache[keymin]

  #FIXME: this should implement gfreq and lfreq parts.
  def evictLORE(self, key, dc):
    directory = dc.blk_dir
    minlfreq = 10000
    mingfreq = 0
    evictKey = ""
    lastCandidKey = ""
    if (key not in directory.dict) or directory.dict[key]['valid'] == 0:
        for candidKey in self.cache:
            value = self.cache[candidKey]
            lfreq = value["lfreq"]
            if lfreq < minlfreq:
                minlfreq = lfreq
                lastCandidKey = candidKey
                if (directory.dict[candidKey]['gfreq'] == 0) or (len(directory.dict[candidKey]['location']) > 1):
                    evictKey = candidKey
        if evictKey == "":
            evictKey = lastCandidKey
            self.moveToRemoteLORE(evictKey, directory.dict[evictKey]['size'], dc)
    else:
        for candidKey in self.cache:
            value = self.cache[candidKey]
            lfreq = value["lfreq"]
            if lfreq < minlfreq:
                minlfreq = lfreq
                if (directory.dict[candidKey]['gfreq'] == 0) or (len(directory.dict[candidKey]['location']) > 1):
                    evictKey = candidKey

    if evictKey != "":
        #print("AMIN: evict key is not empty and is %s" %evictKey)
        self.free_space += int(self.cache[evictKey]['size'])
        directory.removeBlock(evictKey, self.name)
        del self.cache[evictKey]
        #print("AMIN: EvictLORE keys after the evict is are %s" %self.cache.keys())
        return True
    else:
        #print("AMIN: evict key is empty, return FALSE")
        return False


  def evictFIFO(self):
    oid = self.cache.get()
    #directory.removeBlock(oid, self.name)
    self.free_space += int(self.cache[oid]['size'])



  def moveToRemoteLORE(self, evictKey, size, dc):
      for i in dc.cache_layer:
          if dc.cache_layer[i].free_space >= dc.cache_layer[i].size/2:
              if evictKey not in dc.cache_layer[i]:
                dc.cache_layer[i].insertLORE(evictKey, size, dc)
                #print("LORE: Remote cache is %s" %i)
                return


  def halve_lfreq(self):
    for key in self.cache:
      """
      value = self.cache[key]["lfreq"]/2 
      lfreq = value["lfreq"]
      value["lfreq"] = int(lfreq/2)
      self.cache[key] = value
      """
      self.cache[key]["lfreq"] /= 2

  def print(self):
    if (self.policy == Cache.LRU):
      print(self.name, "LRU", self.cache.items())
    elif (self.policy == Cache.LORE):
      print(self.name, "LORE", self.cache.items())
    elif (self.policy == Cache.FIFO):
      print(self.name, "FIFO", list(self.cache.queue))

  def remove(self,key, directory):
    if (self.policy == Cache.LRU):
      del self.cache[key]
    if (self.policy == Cache.LORE):
      del self.cache[key]
      directory.remove_block_entry(key)
    elif (self.policy == Cache.FIFO):
      del self.cache[key]
