import pandas as pd
import numpy as np
class JobStat:
  def __init__(self):
    col_names = ['jobid', 'objname', 'starttime', 'endtime', 'size', 'iotype', 'owner', 'mapper', 'workflowid', 'completed_tasks', 'done']
    self.df =  pd.DataFrame(columns = col_names)
    self.df = self.df.set_index(['jobid']) 
   
  def insert(self, key, job, time):
    self.df.loc[key] = [job.objname, time, np.nan , job.size, job.iotype, job.client, job.mapper, job.workflowid, 0, False]

  def update(self, key, time):
    count  =  self.df.loc[key, 'completed_tasks']
    count += 1
    self.df.loc[key, 'completed_tasks'] = count
    if (count == self.df.loc[key,'mapper']):
      self.df.loc[key, 'endtime'] = time
      self.df.loc[key, 'done'] = True
      
  def isFinished(self, key):
    if self.df.loc[key,'done'] == True:
      return True;
    else : 
      return False;

  def inProgress(self, key):
    subset = self.df.loc[self.df['objname'] == key]
#    subset2 = subset.loc[subset['iotype'] == 'read']
 #   print('inprogres-----------')
 #   print(subset)
#    print(subset2)
 #   print('-----------')
    return subset.endtime.isnull().any()

  def inProgressWrite(self,key):
    subset = self.df.loc[self.df['objname'] == key]
    subset2 = subset.loc[subset['iotype'] == 'write']
    return subset2.endtime.isnull().any()
