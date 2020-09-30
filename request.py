
class Request:
        def __init__(self, rid, mapper_id, task, source, dest, path, offset, size, rtype):
                self.req_id=rid
                self.mapper_id=mapper_id
                self.task=task
                self.job=task.job
                self.source=source
                self.dest = dest
                self.offset = offset
                self.end = offset+size
                self.size= size
                self.path = path
                self.rtype = rtype
                self.startTime = 0
                self.endTime = 0
                self.compTime = 0
                self.info = None
                self.name = task.job.objname+"_"+str(offset)
        def set_startTime(self,time):
                self.startTime = time
        def set_endTime(self,time):
                self.endTime = time
        def set_compTime(self,time):
                self.compTime = time
        def set_info(self,info):
                self.info = info
        def set_source(self,source):
                self.source = source
        def set_fetch(self,layer):
                self.fetch = layer
        def set_destination(self,dest):
                self.dest = dest
        def get_source(self):
                return self.source
        def get_compTime(self):
                return self.compTime
        def get_fetch(self):
                return self.fetch
        def get_destination(self):
                return self.dest
        def get_info(self):
                return self.info
