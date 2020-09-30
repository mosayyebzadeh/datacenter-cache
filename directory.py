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

  def remove(self, key, location):
    loc = self.df.loc[key,'location']
    if len(loc) == 1: #final copy
      self.df.delete(key)
    else: #more copies just update location field
      loc.remove(location)
      self.df.at[key, 'location'] = loc

  def get_location(self, key):
    dest = self.df.loc[key].location
    if isinstance(dest, list):
      return random.choice(dest)
    else:
      return dest

  def aged_items(self, time, count):
    write_items= self.obj_df[self.obj_df['location'].apply(lambda x: 'writeCache' in x)]
    sort_by_ctime = write_items.sort_values('c_time',ascending=False)
    return sort_by_ctime

