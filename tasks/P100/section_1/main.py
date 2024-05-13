from core.ports import Process
import time

class SequentialDag(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_target = kwargs["project_target"]
    
    def run(self):
        self._sleep()
    
    def _sleep(self):
        time.sleep(60)