import random
from timer import Timer
class Directory:
  def __init__(self, name):
    self.name = name
    self.size = 0
    self.free_space = 0
    self.interval = 0
    self.threshold = 0
    self.count = 0
    self.dict = {}
    self.dirTimer = Timer()

  def haskey(self, key):
    if key in self.dict:  
      return True
    else:
      return False

  def put(self, key, size, location):
    #print("directory put")
    #self.dirTimer.start()
    if key in self.dict:
        value = self.dict[key]
        #value['valid'] = 1 # update global access freq
        value['location'].add(location)
    else:
        value = {'size': size, 'location': {location}, 'gfreq': 1}
    self.dict.update({key: value}) 
    self.free_space -= 1
    #self.dirTimer.stop()

  def updateGFreq(self, key):
    if key in self.dict:  
        self.dict[key]['gfreq'] += 1 # update global access freq

  def updateGFreqLFUDA(self, key, value):
    if key in self.dict:  
        self.dict[key]['gfreq'] =value # update global access freq


  """
  def updateTime(self, key, time):
    if key in self.dict.keys():  
        self.dict[key]['la_time'] = time # update global access freq
  """

  def removeBlock(self, key, location):
    #print("directory removeBlock")
    #self.dirTimer.start()
    value = self.dict[key]
    loc = value['location']
    #print("directory removeBlock key and location is: ", key, loc)
    if len(loc) <= 1: #final copy
        #value['valid'] = 0
        #loc.remove(location)
        #value['location'] = loc
        del self.dict[key]

    else:
        loc.remove(location)
        value['location'] = loc

        self.dict.update({key: value}) #There is no cache having the data
    #self.dirTimer.stop()
    #for k in self.dict.keys():
    #    print(k, self.dict[k]['gfreq'], self.dict[k]['location'])


  def get_all_blk_location(self,key):
    temp = self.dict[key]['location']
    return temp

  #FIXME: should we choose a better cache location?
  def get_location(self, key):
    dest = self.dict[key]['location']
    return next(iter(dest))
    """
    if isinstance(dest, list):
      return random.choice(dest)
    else:
      return dest
    """

  def halve_gfreq(self):
      for key in self.dict:
        self.dict[key]['gfreq'] = int(self.dict[key]['gfreq']/2)

  def removeEntry(self):
    #print('AMIN remove entry keys are: %s' %self.df.index)
    if (self.size - self.free_space >= self.threshold):
      for key in self.dict:
        if len(self.dict[key]['location']) == 0:
          #print('AMIN remove entry: found an invalid key: %s' %key)
          del self.dict[key]
          self.free_space += 1
          if (self.size - self.free_space < self.threshold):
            break

