import pandas as pd
import networkx as nx
'''['state', 'submitTime', 'startTime', 'finishTime', 'queueTime',
       'runTime', 'NumMaps', 'avgMapTime', 'avgReduceTime', 'avgShuffleTime',
       'avgMergeTime', 'NumReduce', 'HDFS_INPUT_SIZE', 'HDFS_OUTPUT_SIZE',
       'MAP_CPU_USAGE_MSEC', 'REDUCE_CPU_USAGE_MSEC', 'MAP_MEM_USAGE_B',
       'REDUCE_MEM_USAGE_B', 'HIVE_RECORDS_IN', 'HIVE_RECORDS_OUT',
       'HIVE_RECORDS_INTERMEDIATE', 'SLOTS_MILLIS_MAPS',
       'SLOTS_MILLIS_REDUCES', 'TOTAL_LAUNCHED_MAPS', 'TOTAL_LAUNCHED_REDUCES',
       'DATA_LOCAL_MAPS', 'RACK_LOCAL_MAPS', 'MILLIS_MAPS', 'MILLIS_REDUCES',
       'VCORES_MILLIS_MAPS', 'VCORES_MILLIS_REDUCES', 'MB_MILLIS_MAPS',
       'MB_MILLIS_REDUCES', 'PHMAP_MEM_USAGE_B', 'PHREDUCE_MEM_USAGE_B',
       'PHPHYSICAL_MEMORY_B', 'jobid', 'job.maps', 'query', 'outputdir',
       'scratchdir', 'sessionid', 'query.id', 'local.scratchdir', 'tmpouput',
       'user.name', 'job', 'n_inputs', 'inputdir', 'workflow.node',
       'workflow.id', 'workflow.dag', 'table.name', 'submit_ts',
       'submit_ts_p10m']
'''

def getDag(df):
  print(getDag.__name__)
  df_dep = df[['startTime','inputdir','outputdir','workflowid']]
  dags = {}
  workflow_list = df_dep.workflowid.unique()
  for key in workflow_list:
    workflow = df_dep.loc[(df['workflowid'] == key)]
    workflow = workflow.sort_values('startTime')
    workflow = workflow.drop(['startTime', 'workflowid'], axis=1)
    edges = workflow.values.tolist()
    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    dags[key]=graph
  for k in dags.keys():
    print(k,'->',list(nx.topological_sort(dags[k])))


def traceParser(filename):
#  df = pd.read_csv(filename, sep=',',skipinitialspace=True, header = None, nrows=5, usecols=[2,3,12,13,39,45,48,50])
  df = pd.read_csv(filename, sep=',',skipinitialspace=True, header = None,  usecols=[0,1,2,3,4,5,6])
 # df.columns = ['startTime','finishTime','mapper','input_size','output_size','outputdir','user_name','inputdir','workflowid','iotype']

  df.columns = ['startTime','mapper','input_size','inputdir','user_name','workflowid','iotype']
  df.loc[df['iotype'] == 'delete', ['mapper']] = 1 
  # round input to multiple of 4

  base = 4
  df['mapper_input_size'] = base * round( (df.input_size / df.mapper)/base)
  df.mapper_input_size = df.mapper_input_size.astype(int)
  df.input_size = df.input_size.astype(int)
  print(df)
  return df

#def createFinalTrace(df):
  df_r = df[['startTime','user_name','inputdir', 'input_size','workflowid']]
  df_w = df[['finishTime','user_name','outputdir', 'output_size','workflowid']]
  del df
  df_r = df_r.rename(columns={'inputdir':'objname', 'input_size':'size'})
  df_w = df_w.rename(columns={'finishTime':'startTime','outputdir':'objname', 'output_size':'size'})
  df_r['io'] = 'read'
  df_w['io'] = 'write'
  frames = [df_r, df_w]
  df = pd.concat(frames)

  # Create Deletion for intermediate datasets
  workflow_list = df.workflowid.unique()  
  
  G = nx.DiGraph()
  G.add_edge(1,2)
  dags = {}
  for key in workflow_list:
    workflow1 = df.loc[(df['workflowid'] == key)]
    workflow1 = workflow1.sort_values('startTime') 
    print(workflow1)
    workflow = df.loc[(df['workflowid'] == key) & (df['io'] == 'write')]
    workflow = workflow.sort_values('startTime') 
    query_completion_time = workflow.startTime.tail(1) + 1

    workflow = workflow.iloc[:-1]
#    print(workflow)
    write_list = workflow.loc[workflow['io'] == 'write']
    print("----")
    inputs = workflow1.objname.values.tolist()
    print(inputs)
    
  #df = df.sort_values('startTime')

  #print(df)
  del df_r, df_w
  #df['startTime']= pd.to_datetime(df['startTime'])








