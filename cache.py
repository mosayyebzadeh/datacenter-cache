from lru import LRU
import queue
#"""Class representing wb-cache"""
class Cache:

  # Replacement policies
  LRU = "LRU"
  FIFO = 'FIFO'
  LORE = 'LORE'
  LFUL = 'LFUL'
  LFUG = 'LFUG'

  def __init__(self, name, size, policy, interval):
    self.name = name
    self.size = size
    self.interval = interval
    self.free_space = size
    self.policy = policy # Eviction policy
    self.hashmap = {} # Mapping <objname,objsize>

    if (self.policy == Cache.LRU):
      self.cache = LRU(self.size)
    elif (self.policy == Cache.LFUL):
      self.cache = {}
    elif (self.policy == Cache.LFUG):
      self.cache = {}
    elif (self.policy == Cache.LORE):
      self.cache = {}
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
    if key in self.hashmap.keys():
      return True
    else:
      return False

  def put(self, key, size, time, directory):
    if self.has_key(key):
      #print("AMIN: PUT has key %s keys are %s" %(self.name, self.cache))
      self.update(key,size, time, directory)
    else:  
      #print("AMIN: PUT not key %s key is %s" %(self.name, key))
      #print("AMIN: PUT not key %s keys are %s" %(self.name, self.cache))
      #self.halve_freq(directory, time)
      self.insert(key, size, time, directory)
    """
    print("*********************")
    self.print()
    print("DIRECTORY:", directory.dict)
    print("#####################")
    """

  def update(self,key,size, time, directory):
    self.hashmap[key] = size
    self.hit_count += 1;    
    #print("AMIN: Update %s key is %s" %(self.name, key))
    if (self.policy == Cache.LRU):
        value = self.cache[key]
        locations = value["location"]
        if self.name not in locations:
            locations.append(self.name)
        value = {"size": size, "lfreq":1, "location":locations, "time":0}
        self.cache[key] = value

    elif (self.policy == Cache.LFUL):
        value = self.cache[key]
        lfreq = value["lfreq"] + 1
        locations = value["location"]
        if self.name not in locations:
            locations.append(self.name)
        value = {"size": size, "lfreq":lfreq, "location":locations, "time":time}
        self.cache.update({key: value})

    elif (self.policy == Cache.LFUG):
        directory.put(key, size, self.name, time)

    elif (self.policy == Cache.LORE):
        value = self.cache[key]
        lfreq = value["lfreq"] + 1
        locations = value["location"]
        if self.name not in locations:
            locations.append(self.name)
        value = {"size": size, "lfreq":lfreq, "location":locations, "time":time}
        self.cache.update({key: value})
        directory.updateTime(key, time)
    elif (self.policy == Cache.FIFO):
        self.cache.put(key)

  def insert(self, key, size, time, directory):
    if (self.policy == Cache.LRU):
      self.insertLRU(key, size)
    elif (self.policy == Cache.LFUL):
      self.insertLFUL(key, size, time)
    elif (self.policy == Cache.LFUG):
      self.insertLFUG(key, size, time, directory)
    elif (self.policy == Cache.LORE):
      self.insertLORE(key, size, time, directory)
    elif (self.policy == Cache.FIFO):
      self.insertFIFO(key, size, directory)

 
  def insertLRU(self, key, size):
    while(int(size) > self.free_space):
      self.evictLRU()
    lfreq = 1
    value = {"size": size, "lfreq":1, "location":self.name, "time":0}
    self.cache[key] = value
    self.hashmap[key] = size
    self.free_space -= size
    self.miss_count +=1

  def insertLFUL(self, key, size, time):
    while(int(size) > self.free_space):
        self.evictLFUL()
    value = {"size": size, "lfreq":1, "location":self.name, "time":time}
    self.cache.update({key: value})
    self.hashmap[key] = size
    self.free_space -= size
    self.miss_count +=1

  def insertLFUG(self, key, size, time, directory):
    while(int(size) > self.free_space):
        self.evictLFUG(directory)
    value = {"size": size, "lfreq":1, "location":self.name, "time":time}
    self.cache.update({key: value})
    #print("AMIN: INSERTLORE Key is %s and value is %s" %(key, value))
    self.hashmap[key] = size
    directory.put(key, size, self.name, time)
    self.free_space -= size
    self.miss_count +=1

  #value is a dictionary (size, lfreq, location, time)
  def insertLORE(self, key, size, time, directory):
    #print("AMIN: size is %d and free_space is %d" %(int(size), self.free_space))
    #print("AMIN: Insert %s key is %s" %(self.name, key))
    while(int(size) > self.free_space):
        if not self.evictLORE(key, directory):
            return
    value = {"size": size, "lfreq":1, "location":self.name, "time":time}
    self.cache.update({key: value})
    #print("AMIN: INSERTLORE Key is %s and value is %s" %(key, value))
    self.hashmap[key] = size
    directory.put(key, size, self.name, time)
    self.free_space -= size
    self.miss_count +=1

  def insertFIFO(self, key, size, directory):
    while(int(size) >= self.free_space):
        self.evictFIFO(directory)
    self.cache.put(key)
    self.hashmap[key] = size
    self.free_space -= size
    self.miss_count +=1

  def evictLRU(self):
    oid = self.cache.peek_last_item()[0]
    self.free_space += int(self.hashmap[oid])
    #directory.removeBlock(oid, self.name)
    del self.hashmap[oid]     
    del self.cache[oid]

  def evictLFUL(self):
    keyMin = min(self.cache, key= lambda x: self.cache[x]['lfreq'])
    self.free_space += int(self.hashmap[keyMin])
    del self.hashmap[keyMin]     
    del self.cache[keyMin]

  #FIXME: we need to make sure the found key is present in the local cache
  def evictLFUG(self, directory):
      #if Keymin = min(directory, key= lambda x: directory[x]['gfreq']) in self.cache.keys():
        self.free_space += int(self.hashmap[keymin])
        del self.hashmap[keymin]     
        del self.cache[keymin]

  #FIXME: this should implement gfreq and lfreq parts.
  def evictLORE(self, key, directory):
    minlfreq = 10000
    mingfreq = 0
    evictKey = ""
    lastCandidKey = ""
    #print("EvictLORE key to be inserted is %s" %key)
    #print("EvictLORE %s before keys are %s" %(self.name, self.cache.items()))
    #print("EvictLORE DIRECTORY is %s" %directory.dict.keys())
    if (not directory.haskey(key)) or directory.dict[key]['valid'] == 0:
        for candidKey in self.cache.keys():
            #print("AMIN: candid key is %s" %candidKey)
            value = self.cache[candidKey]
            lfreq = value["lfreq"]
            #print("AMIN: lfreq for candid key %s is %d" %(evictKey, lfreq))
            if lfreq < minlfreq:
                minlfreq = lfreq
                #print("AMIN: 1")
                lastCandidKey = candidKey
                #print("key is %s, evict key is %s, last candid Key is %s" %(key, evictKey, lastCandidKey))
                if (directory.dict[candidKey]['gfreq'] == 0) or (len(directory.dict[candidKey]['location']) > 1):
                    #print("AMIN: inside the conditions ---------------->")
                    evictKey = candidKey
                    #print("AMIN: evict key1 is %s" %evictKey)
        if evictKey == "":
            #print("AMIN: evict key is empty")
            evictKey = lastCandidKey
            #print("AMIN: evict key is %s" %evictKey)
    else:
        #print("AMIN: cached some where")
        for candidKey in self.cache.keys():
            value = self.cache[candidKey]
            lfreq = value["lfreq"]
            #print("AMIN: lfreq for candid key %s is %d" %(evictKey, lfreq))
            if lfreq < minlfreq:
                minlfreq = lfreq
                #print("AMIN: 2")
                if (directory.dict[candidKey]['gfreq'] == 0) or (len(directory.dict[candidKey]['location']) > 1):
                    evictKey = candidKey
                    #print("AMIN: evict key2 is %s" %evictKey)

    if evictKey != "":
        #print("AMIN: evict key is not empty and is %s" %evictKey)
        self.free_space += int(self.hashmap[evictKey])
        directory.removeBlock(evictKey, self.name)
        del self.hashmap[evictKey]
        del self.cache[evictKey]
        #print("AMIN: EvictLORE keys after the evict is are %s" %self.cache.keys())
        return True
    else:
        #print("AMIN: evict key is empty, return FALSE")
        return False


  def evictFIFO(self, directory):
    oid = self.cache.get()
    directory.removeBlock(oid, self.name)
    self.free_space += int(self.hashmap[oid])
    del self.hashmap[oid]     

  def halve_lfreq(self):
    for key in self.cache.keys():
      value = self.cache[key]
      lfreq = value["lfreq"]
      value["lfreq"] = int(lfreq/2)
      self.cache[key] = value

  """
  def halve_freq(self, directory, time):
    #print("AMIN: HALVE_FREQ time is %s" %time)
    self.halve_lfreq(time)
    for key in directory.df.index:
      owner = directory.get_owner(key)
      if owner == self.name:
        directory.halve_gfreq(key)

  def halve_lfreq(self, key):
    for key in self.cache.keys():
      value = self.cache[key]
      #print("AMIN: HALVE_LLLLFREQ: %s key is %s value[lfreq] is %s" %(self.name, key, value["lfreq"]))
      lfreq = value["lfreq"]
      value["lfreq"] = int(lfreq/2)
      self.cache[key] = value
      #print("AMIN: HALVE_LLLLFREQ2: %s key is %s value[lfreq] is %d cache[lfreq] is %d" %(self.name, key, value["lfreq"], self.cache[key]["lfreq"]))
  """



  def print(self):
    if (self.policy == Cache.LRU):
      print(self.name, "LRU", self.hashmap, self.cache.items())
    elif (self.policy == Cache.LORE):
      print(self.name, "LORE", self.hashmap)
    elif (self.policy == Cache.FIFO):
      print(self.name, "LRU", self.hashmap, list(self.cache.queue))

  def remove(self,key, directory):
    del self.hashmap[key]
    if (self.policy == Cache.LRU):
      del self.cache[key]
    if (self.policy == Cache.LORE):
      del self.cache[key]
      directory.remove_block_entry(key)
    elif (self.policy == Cache.FIFO):
      a=5
