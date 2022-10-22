from dataclasses import dataclass
from os import getpid
from ctypes import c_double, c_bool
from multiprocessing import Process, Queue, connection, Lock, Value
import time
from typing import Callable


@dataclass
class JobData:
    """Data needed for running a single job"""

    args: tuple
    # Pipe end to which output should be sent
    output_connection: connection.Connection


class Worker(Process):
    def __init__(self, job_function: Callable):
        super(Worker, self).__init__()
        self.job_queue = Queue()
        self.lock = Lock()
        self._last_job_work_time = Value(c_double, time.time())
        self._is_running = Value(c_bool, False)
        self._job_function = job_function

    @property
    def is_busy(self):
        return self._is_running.value

    @property
    def last_job_work_time(self):
        return self._last_job_work_time.value

    def add_job(self, job_data: JobData):
        self.job_queue.put(job_data)

    def run(self):
        while True:
            if not self.job_queue.empty():
                with self.lock:
                    print(f'Worker {getpid()} is taking a job')
                    self._is_running.value = True
                    item = self.job_queue.get(block=True)
                    try:
                        self._job_function(item)
                    finally:
                        self._is_running.value = False
                        self._last_job_work_time.value = time.time()
