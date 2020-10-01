import configparser, cache, argparse, logging, pprint, datetime
import simpy, threading, multiThread, event
from cache import Cache
from traceParser import * 
from dataCenter import DataCenter 
from scheduler import Scheduler 


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Simulate a cache')
  parser.add_argument('-t','--trace-file', help='Storage access trace file', required=True)
  parser.add_argument('-c','--config-file', help='Configuration file for datacenter topology', required=True)
  arguments = vars(parser.parse_args())
  trace_file = arguments['trace_file']
  
  config = configparser.ConfigParser()
  config.read_file(open(arguments['config_file']))

  log_filename = 'wb_sim.log'
#  time=str(datetime.datetime.now()).replace(" ","_")
#  log_filename+=time  
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
  directory = {}
  dc = DataCenter("datacenter1")
  dc.build(config, logger, env) 
  dc.scheduler = Scheduler(dc.compute_nodes, dc.cpu, directory, dc.mapper_list, dc.cache_layer)
   
 
  logger.info('Parsing Trace File...')
  print("Parsing Trace File...")
  df = traceParser(trace_file)
  logger.info('Generating Final Trace File...')
  print("Generating Final Trace File...")
  

  logger.info('Running Simulation')
  print('Running Simulation')
    
  dc.scheduler.addJobs(df) 
#  dc.scheduler.allocateJob() 
#  dc.scheduler.addJobs(df) 
#  dc.scheduler.start()
  #s_thread = threading.Thread(target=dc.scheduler.start2(env))
  #s_thread.start()
  print("first jobs allocated") 
  # Thread pool for mappers
  pool = multiThread.ThreadPool(len(dc.mapper_list.keys()))
  for i in dc.mapper_list.keys():
    pool.add_task(event.request_generator, i, dc, dc.scheduler, env)
  pool.wait_completion()
  env.run()
  print(dc.blk_dir.df) 
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
  dc.cache_layer['writeCache'].print()
  dc.cache_layer['cache0'].print()
  dc.cache_layer['cache1'].print()
#  s_thread.join()
