import random
class Directory:
  def __init__(self, name):
    self.name = name
    self.df = None
    self.obj_df = None

  def haskey(self, key):
    if key in self.df.index:  
      return True
    else:
      return False

  def put(self, key, req, location, time):
    if self.haskey(key): # update directory another copy exists
      self.update(key, req, location, time)
    else: # new record
      self.insert(key, req, location, time)

  def update(self, key, req, location, time):
    self.df.at[key, 'la_time'] = time #update last access time
    loc = self.df.loc[key,'location'] # add new location
    if location not in loc: # miss 
      loc.append(location)
      self.df.at[key, 'location'] = loc
    else: # means hit 
      self.df.at[key, 'freq'] = self.df.loc[key,'freq'] + 1 # update access freq
   

  def insert(self, key, req, location, time):
    self.df.loc[key] = [time, req.size, [location], req.job.client, 1 , time ]

  def insertObj(self, key, size, client, time):
    self.obj_df.loc[key] = [time, size, 'writeCache', client, 1 , time ]

  def removeBlock(self, key, location):
    loc = self.df.loc[key,'location']
    if len(loc) == 1: #final copy
      self.df.delete(key)
    else: #more copies just update location field
      loc.remove(location)
      self.df.at[key, 'location'] = loc

  def get_all_blk_location(self,key):
    temp = self.df.loc[self.df.index.str.contains(key)]
    return temp.location

  def get_all_obj_location(self,key):
    return self.obj_df.loc[key].location    

  def get_location(self, key):
    dest = self.df.loc[key].location
    if isinstance(dest, list):
      return random.choice(dest)
    else:
      return dest

  def aged_items(self, time, count):
    sort_by_ctime = self.obj_df.sort_values('c_time',ascending=True).head(count)
    return list(sort_by_ctime.index)

  def remove_block_entry(self, key):
    self.df = self.df.drop(key)
    print(key)
    print(self.df)
    print('---') 
  def remove_obj_entry(self, key):
    self.obj_df = self.obj_df.drop(key)
