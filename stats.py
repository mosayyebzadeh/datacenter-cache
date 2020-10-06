import pandas as pd
import numpy as np
class JobStat:
  def __init__(self):
    col_names = ['jobid', 'objname', 'startTime', 'endTime', 'size', 'iotype', 'owner', 'mapper', 'workflowid', 'completed_tasks', 'done', 'completionTime', 'tput']
    self.df =  pd.DataFrame(columns = col_names)
    self.df = self.df.set_index(['jobid']) 
   
  def insert(self, key, job, time):
    self.df.loc[key] = [job.objname, time, np.nan , job.size, job.iotype, job.client, job.mapper, job.workflowid, 0, False, 0, 0]

  def update(self, key, time):
    count  =  self.df.loc[key, 'completed_tasks']
    count += 1
    self.df.loc[key, 'completed_tasks'] = count
    if (count == self.df.loc[key,'mapper']):
      self.df.loc[key, 'endTime'] = time
      self.df.loc[key, 'done'] = True
      self.df.loc[key, 'completionTime'] =  self.df.loc[key, 'endTime'] - self.df.loc[key, 'startTime']     
      if  self.df.loc[key, 'completionTime'] != 0:
        self.df.loc[key, 'tput'] =  float(self.df.loc[key, 'size']) / self.df.loc[key, 'completionTime']     
 
  def isFinished(self, key):
    if self.df.loc[key,'done'] == True:
      return True;
    else : 
      return False;

  def inProgress(self, key):
    subset = self.df.loc[self.df['objname'] == key]
    return subset.endTime.isnull().any()

  def inProgressWrite(self,key):
    subset = self.df.loc[self.df['objname'] == key]
    subset2 = subset.loc[subset['iotype'] == 'write']
    return subset2.endTime.isnull().any()
