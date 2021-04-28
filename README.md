# cache-sim: Event-Driven Simulation for object store cache

cache-sim is an datacenter-scale cache architecture simulation for hierarchical network topologies. The simulation framework is implemented using [simpy](https://simpy.readthedocs.io/en/latest/) which is a process-based discrete-event simulation based on Python.

# File Inventory:
  * config.ini - Config File
  * trase0 - Sample Trace File
  * wb_sim.py - Main simulator code which executes the simulator
 
 
# Prerequisites:
Install all the required dependencies:

```
pip install -r requirements.txt
```

# Configuring Simulation For Your Enviroment 
  Edit 'config.ini' for your environment. Certain variables must be configured for your test environment.
 
  
# Input Trace File Format
 * Job_ID,mapper_number,  size, objectName,  userName,    worklodName,  operation

 
 Each line represents "Read, Write, Delete" requests. Simulator read the trace and start issuing these requests.
 To DO: 
 Update write and delete sections

# Usage

```
python3 wb_sim.py -c <configFile>
```
