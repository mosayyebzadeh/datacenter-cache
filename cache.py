from lru import LRU
import queue
from lfuCache import LFUCache
from timer import Timer

class Cache:

  # Replacement policies
  LRU = "LRU"
  LRUD = "LRUD" #LRU with support of directory. we push evicted key to remote and read from remote if possible
  FIFO = 'FIFO'
  LORE = 'LORE'
  LFUL = 'LFUL' #Local LFU. each cache has it is own lfu based eviction policy
  LFUG = 'LFUG'
  LFUD = 'LFUD' #LFU with support of directory. we push evicted key to remote and read from remote if possible
  LFUDA = 'LFUDA' #LFU with dynamic aging. The directory is used to push evicted key to remote and read from remote if possible

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
    elif (self.policy == Cache.LRUD):
      self.cache = LRU(self.size)
    elif (self.policy == Cache.LFUL):
      self.cache = LFUCache(self.size)
    elif (self.policy == Cache.LFUG):
      self.cache = {}
    elif (self.policy == Cache.LORE):
      #value = {"size": 0, "lfreq":0, "location":set()}
      #self.cache = dict.fromkeys(setKeys, value)
      #self.cache.update(dict(self.cache))
      self.cache = {}
    elif (self.policy == Cache.LFUD):
      self.cache = LFUCache(self.size)
    elif (self.policy == Cache.LFUDA):
      self.cache = LFUCache(self.size)
      self.aging = 0
      self.avgFreq = 0
    elif (self.policy == Cache.FIFO):
       self.cache = queue.Queue(maxsize=self.size)

    # Statistics
    self.hit_count = 0
    self.miss_count = 0
    self.miss_size_count = 0
    self.local_hit = 0
    self.local_size_hit = 0
    self.remote_hit = 0
    self.remote_size_hit = 0

  def has_key(self, key):
    if (self.policy == Cache.LFUD) or (self.policy == Cache.LFUL) or (self.policy == Cache.LFUDA):
        if key in self.cache.cache:
          return True
        else:
          return False

    if key in self.cache:
      return True
    else:
      return False

  def put(self, key, size, dataCenter):
    #self.cacheTimer.start()
    if self.has_key(key):
    #if key in self.cache:
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
    #self.hit_count += 1;    
    if (self.policy == Cache.LRU):
        value = self.cache[key]

    elif (self.policy == Cache.LRUD):
        value = self.cache[key]

    elif (self.policy == Cache.LFUL):
        value = self.cache.get(key)

    elif (self.policy == Cache.LFUG):
        dc.blk_dir.put(key, size, self.name)

    elif (self.policy == Cache.LORE):
        self.cache[key]["lfreq"] += 1

    elif (self.policy == Cache.LFUD):
        value = self.cache.get(key)

    elif (self.policy == Cache.LFUDA):
        #print("updating key:", key)
        value = self.cache.getAging(key, self.aging, self, dc)

    elif (self.policy == Cache.FIFO):
        self.cache.put(key)

  def insert(self, key, size, dc):
    if (self.policy == Cache.LRU):
      self.insertLRU(key, size)
    elif (self.policy == Cache.LRUD):
      self.insertLRUD(key, size, dc)
    elif (self.policy == Cache.LFUL):
      self.insertLFUL(key, size)
    elif (self.policy == Cache.LFUG):
      self.insertLFUG(key, size)
    elif (self.policy == Cache.LORE):
      self.insertLORE(key, size, dc)
    elif (self.policy == Cache.LFUD):
      self.insertLFUD(key, size, dc)
    elif (self.policy == Cache.LFUDA):
      self.insertLFUDA(key, size, dc)
    elif (self.policy == Cache.FIFO):
      self.insertFIFO(key, size)

 
  def insertLRU(self, key, size):
    #print("Key ans size is: ", key, size)
    while(int(size) > self.free_space):
        #print("cache free space is: ", self.free_space)
        self.evictLRU()
    self.cache[key] = size
    self.free_space -= size

  def insertLRUD(self, key, size, dc):
    while(int(size) > self.free_space):
      self.evictLRUD(dc)
    self.cache[key] = size
    self.free_space -= size
    dc.blk_dir.put(key, size, self.name)

  def insertLFUL(self, key, size):
    cache_node = self.cache.set(key, size)
    self.free_space -= size


  def insertLFUG(self, key, size):
    while(int(size) > self.free_space):
        self.evictLFUG()
    value = {"size": size}
    self.cache.update({key: value})
    directory.put(key, size, self.name)
    self.free_space -= size

  #value is a dictionary (size, lfreq, location)
  def insertLORE(self, key, size, dc):
    while(int(size) > self.free_space):
        if not self.evictLORE(key, dc):
            return
    value = {"size": size, "lfreq":1}
    self.cache.update({key: value})
    dc.blk_dir.put(key, size, self.name)
    self.free_space -= size

  def insertLFUD(self, key, size, dc):
    cache_node = self.cache.set(key, size)
    self.free_space -= size
    dc.blk_dir.put(key, size, self.name)
    if cache_node:
        self.free_space += cache_node[1]
        dc.blk_dir.removeBlock(cache_node[0], self.name)
        self.moveToRemoteLFUD(cache_node[0], cache_node[1], dc)

  def insertLFUDA(self, key, size, dc):
    #print("insert 1 key: %s aging is: %d" %(key, self.aging))
    self.aging = max(self.aging, dc.globalAge)
    dc.globalAge = self.aging

    """
    print("insertLFUDA before setAge")
    print(self.cache.cache.keys())
    for keyP in self.cache.cache.keys():
        print(self.cache.cache[keyP].freq_node.freq)
    """
    cache_node = self.cache.setAging(key, size, self.aging, self, dc)
    """
    print("insertLFUDA after setAge")
    print(self.cache.cache.keys())
    for keyP in self.cache.cache.keys():
        print(self.cache.cache[keyP].freq_node.freq)
   """ 

    self.free_space -= size
    if cache_node:
        #print("insert 3 cache_node is: ", cache_node[0])
        self.aging = max(self.aging, cache_node[2])
        dc.globalAge = self.aging
        self.free_space += cache_node[1]
        #FIXME: update the directory to keep the record
        self.moveToRemoteLFUDA(cache_node[0], cache_node[1], dc)
        dc.blk_dir.removeBlock(cache_node[0], self.name)



  def insertFIFO(self, key, size):
    while(int(size) >= self.free_space):
        self.evictFIFO()
    self.cache.put(key)
    self.free_space -= size


  def evictLRU(self):
    oid = self.cache.peek_last_item()[0]
    #print("evictKey and its size is: ", oid, int(self.cache[oid]))
    if oid:
        self.free_space += int(self.cache[oid])
        del self.cache[oid]


  def evictLRUD(self, dc):
    oid = self.cache.peek_last_item()[0]
    size = int(self.cache[oid])
    self.free_space += size
    self.moveToRemoteLRUD(oid, size, dc)
    dc.blk_dir.removeBlock(oid, self.name)
    del self.cache[oid]


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
    if (key not in directory.dict): #or directory.dict[key]['valid'] == 0:
        for candidKey in self.cache:
            value = self.cache[candidKey]
            lfreq = value["lfreq"]
            if lfreq < minlfreq:
                minlfreq = lfreq
                lastCandidKey = candidKey
                lastCandidKeyValue = directory.dict[candidKey]
                if (lastCandidKeyValue['gfreq'] == 0) or (len(lastCandidKeyValue['location']) > 1):
                    evictKey = candidKey
        if evictKey == "":
            evictKey = lastCandidKey
            self.moveToRemoteLORE(evictKey, lastCandidKeyValue['size'], dc)
    else:
        for candidKey in self.cache:
            value = self.cache[candidKey]
            lfreq = value["lfreq"]
            if lfreq < minlfreq:
                minlfreq = lfreq
                candidKeyvalue = directory.dict[candidKey]
                if (candidKeyvalue == 0) or (len(candidKeyvalue['location']) > 1):
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

  def moveToRemoteLRUD(self, evictKey, size, dc):
      for i in dc.cache_layer:
          if dc.cache_layer[i].name != self.name:
            #if dc.cache_layer[i].free_space >= dc.cache_layer[i].size/2:
            if dc.cache_layer[i].free_space >= size:
              if evictKey not in dc.cache_layer[i].cache:
                dc.cache_layer[i].insertLRUD(evictKey, size, dc)
                return

  def moveToRemoteLORE(self, evictKey, size, dc):
      for i in dc.cache_layer:
          if dc.cache_layer[i].free_space >= dc.cache_layer[i].size/2:
              if evictKey not in dc.cache_layer[i].cache:
                dc.cache_layer[i].insertLORE(evictKey, size, dc)
                #print("LORE: Remote cache is %s" %i)
                return

  def moveToRemoteLFUD(self, evictKey, size, dc):
      for i in dc.cache_layer:
          if dc.cache_layer[i].name != self.name:
            #if dc.cache_layer[i].free_space >= dc.cache_layer[i].size/2:
            if dc.cache_layer[i].free_space >= size:
              if evictKey not in dc.cache_layer[i].cache.cache:
                dc.cache_layer[i].insertLFUD(evictKey, size, dc)
                return

  def moveToRemoteLFUDA(self, evictKey, size, dc):
    value = dc.blk_dir.dict[evictKey]
    #if len(value['location']) == 1:
    for i in dc.cache_layer:
          #print("moving around key: ", evictKey)
          if dc.cache_layer[i].name != self.name:# and value['gfreq'] > dc.cache_layer[i].avgFreq:
            if dc.cache_layer[i].free_space >= size:
                dc.cache_layer[i].insertLFUDA(evictKey, size, dc)
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
