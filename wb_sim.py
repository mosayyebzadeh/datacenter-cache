import configparser, cache, argparse, logging, pprint, datetime
import simpy, threading, multiThread, event
from cache import Cache
from traceParser import * 
from dataCenter import DataCenter 
from scheduler import Scheduler 
import timeit

if __name__ == '__main__':
  start = timeit.default_timer()
  parser = argparse.ArgumentParser(description='Simulate a cache')
  #parser.add_argument('-t','--trace-file', help='Storage access trace file', required=True)
  parser.add_argument('-c','--config-file', help='Configuration file for datacenter topology', required=True)
  arguments = vars(parser.parse_args())
  #trace_file = arguments['trace_file']
  
  config = configparser.ConfigParser()
  config.read_file(open(arguments['config_file']))

  log_filename = 'wb_sim.log'
  with open(log_filename, 'w'):
    pass
  logger = logging.getLogger()
  fh = logging.FileHandler(log_filename)
  logger.addHandler(fh)
  logger.setLevel(logging.DEBUG)
  
  logger.info('Loading config...')
  print ('Loading config...')
  
  logger.info('Creating Enviroment...')
  print ('Creating Enviroment...') 
  env = simpy.Environment()
  #directory = {}
  dc = DataCenter("datacenter1")
  dc.build(config, logger, env) 
  dc.scheduler = Scheduler(dc.compute_nodes, dc.cpu, dc.blk_dir, dc.mapper_list, dc.cache_layer, dc.jobStat)
   
 
  racks = int(config.get('Simulation', 'cache nodes'))
  logger.info('Parsing Trace File...')

  #df = {}
  print("Parsing Trace File...")
  for i in range(racks):
    trace_file = config.get('Simulation', 'traceFile'+str(i))
    logger.info('Generating Final Trace File...')
    print("Generating Final Trace File...")
    dc.scheduler.addJobs(i, trace_file) 
    #df[i] = traceParser(trace_file)


  logger.info('Running Simulation')
  print('Running Simulation')
    
  #df = traceParser(trace_file)
  #dc.scheduler.addJobs(df, racks) 
  print("first jobs allocated") 
  # Thread pool for mappers
  print(len(dc.mapper_list.keys()))
  pool = multiThread.ThreadPool(len(dc.mapper_list.keys()))
  for i in dc.mapper_list.keys():
    pool.add_task(event.request_generator, i, dc, dc.scheduler, env)

  policy = config.get('Simulation', 'cache policy')
  if policy == "LORE":
    pool.add_task(event.agingFunc, dc, env, dc.interval)
  #pool.add_task(event.cleanUpDir, dc, env, float(config.get('Directory', 'cleanup interval')))

  pool.wait_completion()
  env.run(until = config.get('Simulation', 'end'))
  #env.run()
  """
  sort_by_ctime = dc.blk_dir.df.sort_values('c_time',ascending=False) 
  print('---------sorted--------------')
  print(sort_by_ctime)
  print('---------jobs--------------')
  print(dc.jobStat.df)
  print('----------wb-cache----------')
  print(dc.blk_dir.obj_df.sort_values('c_time',ascending=False))
  print('----------osd-mapping----------')
  print(dc.osdMap)
  print('----------print cache----------')
  """
  print('----------Datalake ----------')
  print("Datalake access is %s" %(dc.dl_access))
  hit_count = 0
  miss_count = 0
  for i in range(dc.c_nodes):
    c_name = "cache"+str(i) #i is rack id
    hit_count += dc.cache_layer[c_name].hit_count
    miss_count += dc.cache_layer[c_name].miss_count
    print("HIT COUNT for cache %s is %s" %( c_name, dc.cache_layer[c_name].hit_count))
    print("Local HIT COUNT for cache %s is %s" %( c_name, dc.cache_layer[c_name].local_hit))
    print("Remote HIT COUNT for cache %s is %s" %( c_name, dc.cache_layer[c_name].remote_hit))
    print("MISS COUNT for cache %s is %s" %( c_name, dc.cache_layer[c_name].miss_count))
    #dc.cache_layer[c_name].print()
  print("Total Hit count is %d" %hit_count)
  print("Total Miss count is %d" %miss_count)
  stop = timeit.default_timer()
  print('Time: ', stop - start)
#  s_thread.join()
