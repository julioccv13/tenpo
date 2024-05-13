from core.ports import Process
import time

class SequentialDag(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_target = kwargs["project_target"]
    
    def run(self):
        self._doSomething()
    
    def _doSomething(self):
        for _ in range(0,10):
            print(f'bucke for {_}')
            time.sleep(1)
        