from functools import partial
from multiprocessing import Pipe, RLock
from threading import Thread
import time
from typing import Callable

from worker_process import Worker, JobData


class WorkerManager:
    def __init__(self, process_timeout: int, worker_function: Callable):
        self._workers = []
        self._process_timeout = process_timeout
        self._worker_list_lock = RLock()
        self._cleaner_thread = self._start_cleaner_thread()
        self._worker_function = partial(WorkerManager.worker_method, inner_function=worker_function)

    @staticmethod
    def worker_method(job_data: JobData, inner_function: Callable) -> None:
        result = inner_function(*job_data.args)
        job_data.output_connection.send(result)

    def _start_cleaner_thread(self) -> Thread:
        cleaner_thread = Thread(target=self.cleaner_thread_proc)
        cleaner_thread.start()
        return cleaner_thread

    def _add_worker(self) -> Worker:
        with self._worker_list_lock:
            worker = Worker(self._worker_function)
            self._workers.append(worker)
            worker.start()
            print(f'Created {worker.pid}')
            return worker

    def get_worker(self) -> Worker:
        with self._worker_list_lock:
            for worker in self._workers:
                if not worker.is_busy:
                    # Possible race that needs to be fixed
                    return worker

            return self._add_worker()

    def cleaner_thread_proc(self):
        while True:
            time.sleep(0.1)
            self.clean_workers(self._process_timeout)

    def clean_workers(self, timeout: int):
        with self._worker_list_lock:
            for worker in self._workers:
                if time.time() - worker.last_job_work_time >= timeout:
                    if worker.lock.acquire(False):
                        print(f'Killing {worker.pid}.')
                        worker.kill()
                        worker.join()
                        self._workers.remove(worker)

    def get_active_processes_pid(self) -> list[int]:
        with self._worker_list_lock:
            return list(map(lambda worker: worker.pid, self._workers))


def run_new_job(worker_manager: WorkerManager, timeout: int, job_args: tuple):
    worker = worker_manager.get_worker()
    receiver, sender = Pipe(duplex=False)
    job = JobData(job_args, sender)
    worker.add_job(job)
    if receiver.poll(timeout=timeout):
        return receiver.recv()
    raise TimeoutError("Waited too long for process to finish. Aborting")
