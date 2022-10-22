from flask import Flask, request

from worker_manager import WorkerManager, run_new_job
from calculation_utils import sleep_and_sum


app = Flask(__name__)
worker_manager = WorkerManager(6, sleep_and_sum)
request_count = 0


@app.route("/active_processes")
def active_processes_handler():
    return str(worker_manager.get_active_processes_pid())


@app.route("/request_count")
def request_count_handler():
    return str(request_count)


@app.route("/sleep_and_sum")
def sleep_and_sum_handler():
    global request_count
    request_count = request_count + 1
    num1 = request.args.get('num1', default=0, type=int)
    num2 = request.args.get('num2', default=0, type=int)
    result = run_new_job(worker_manager, 10, (num1, num2))
    return str(result)


if __name__ == "__main__":
    app.run(port=1337, debug=True)

