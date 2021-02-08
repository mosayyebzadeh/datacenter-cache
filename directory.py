import random
class Directory:
  def __init__(self, name):
    self.name = name
    self.size = 0
    self.free_space = 0
    self.interval = 0
    self.threshold = 0
    self.count = 0
    self.df = None
    #self.obj_df = None

  """
  def haskeyObj(self, key):
    if key in self.obj_df.index:
      return True
    else:
      return False
  """
  def haskey(self, key):
    if key in self.df.index:  
      return True
    else:
      return False

  def put(self, key, size, location, time):
    if self.haskey(key): # update directory another copy exists
      self.update(key, size, location, time)
    else: # new record
      self.insert(key, size, location, time)

  def update(self, key, size, location, time):
    self.df.at[key, 'la_time'] = time #update last access time
    loc = self.df.at[key,'location'] # add new location
    if location not in loc: # miss 
      loc.append(location)
      self.df.at[key, 'location'] = loc
    else: # means hit 
      self.df.at[key, 'gfreq'] = self.df.at[key,'gfreq'] + 1 # update global access freq
   

  def insert(self, key, size, location, time):
    self.free_space -= 1
    gfreq = 1
    valid = 1
    self.df.at[key] = [time, size, [location], gfreq, valid, time]

  """
  def insertObj(self, key, size, client, time):
    self.obj_df.at[key] = [time, size, 'writeCache', client, 1 , time ]
  """
  def removeBlock(self, key, location):
    loc = self.df.at[key,'location']
    if len(loc) == 1: #final copy
      #self.df.drop(key)
      self.df.at[key, 'valid'] = 0 #There is no cache having the data
    else: #more copies just update location field
      loc.remove(location)
      self.df.at[key, 'location'] = loc

  """
  def get_owner(self, key):
    owner = self.df.at[key, "owner"]
    return owner
  """

  def get_all_blk_location(self,key):
    temp = self.df.at[self.df.index.str.contains(key)]
    #print("get", key)
    #print(temp)
    #print('----------','def')
    #print(self.df)
    return temp.location

  """
  def get_all_obj_location(self,key):
    return self.obj_df.at[key].location    
  """
  #FIXME: should we choose a better cache location?
  def get_location(self, key):
    dest = self.df.at[key, 'location']
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
    self.df['gfreq'] = self.df['gfreq']/2

  """
  def aged_items(self, time, count):
    sort_by_ctime = self.obj_df.sort_values('c_time',ascending=True).head(count)
    return list(sort_by_ctime.index)
  """

  def removeEntry(self):
    #print('AMIN remove entry keys are: %s' %self.df.index)
    if (self.size - self.free_space >= self.threshold):
      #print('AMIN remove entry: directory is almost full')
      sort_by_lastAccessTime = self.df.sort_values('la_time',ascending=True).head(self.count)
      keys =  list(sort_by_lastAccessTime.index)
      #print('AMIN remove entry: candidate lru keys are %s' %keys)
      for key in keys:
        if self.df.at[key, 'valid'] == 0:
          #print('AMIN remove entry: found an invalid key: %s' %key)
          self.df = self.df.drop(key)
          self.free_space += 1
          if (self.size - self.free_space < self.threshold):
            break

  """
  def remove_obj_entry(self, key):
    self.obj_df = self.obj_df.drop(key)
  """
