import random
class Directory:
  def __init__(self, name):
    self.name = name
    self.size = 0
    self.free_space = 0
    self.interval = 0
    self.threshold = 0
    self.count = 0
    self.dict = {}

  def haskey(self, key):
    if key in self.dict.keys():  
      return True
    else:
      return False

  def put(self, key, size, location, time):
    value = self.dict.setdefault(key, {'c_time': time, 'size': size, 'location': [location], 'gfreq': 1, 'valid': 1, 'la_time': time})
    value['la_time'] = time #update last access time
    #value['gfreq'] = value['gfreq'] + 1 # update global access freq
    value['valid'] = 1 # update global access freq
    loc = value['location'] # add new location
    if location not in loc: # miss
      loc.append(location)
      value['location'] = loc
    self.dict.update({key: value}) 
    self.free_space -= 1

  def updateGFreq(self, key):
    if key in self.dict.keys():  
        self.dict[key]['gfreq'] +=1 # update global access freq

  def updateTime(self, key, time):
    if key in self.dict.keys():  
        self.dict[key]['la_time'] = time # update global access freq


  def removeBlock(self, key, location):
    value = self.dict.get(key)
    loc = value['location']
    if len(loc) == 1: #final copy
        value['valid'] = 0
        loc = []
        value['location'] = loc
    else:
        loc.remove(location)
        value['location'] = loc

    self.dict.update({key: value}) #There is no cache having the data


  def get_all_blk_location(self,key):
    temp = self.dict.get(key)['location']
    return temp

  #FIXME: should we choose a better cache location?
  def get_location(self, key):
    dest = self.dict.get(key)['location']
    if isinstance(dest, list):
      return random.choice(dest)
    else:
      return dest

  """
  def halving(self, value):
    return value/2

  def halve_gfreq(self):
    self.df['gfreq'] = self.df.apply(
        lambda row: self.halving(
        value=row['gfreq']),
        axis=1)

  """

  def halve_gfreq(self):
      for key in self.dict.keys():
        self.dict[key]['gfreq'] = int(self.dict[key]['gfreq']/2)
  """
  def aged_items(self, time, count):
    sort_by_ctime = self.obj_df.sort_values('c_time',ascending=True).head(count)
    return list(sort_by_ctime.index)
  """

  def removeEntry(self):
    #print('AMIN remove entry keys are: %s' %self.df.index)
    if (self.size - self.free_space >= self.threshold):
      for key in self.dict.keys():
        if self.dict[key]['valid'] == 0:
          #print('AMIN remove entry: found an invalid key: %s' %key)
          del self.dict[key]
          self.free_space += 1
          if (self.size - self.free_space < self.threshold):
            break

  """
  def remove_obj_entry(self, key):
    self.obj_df = self.obj_df.drop(key)
  """
