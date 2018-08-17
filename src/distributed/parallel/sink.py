# Task sink
# Binds PULL socket to tcp://localhost:5558
# Collects results from workers via that socket
#
import atexit
import sys
import time

import zmq

from distributed.utilities import run_with_debugger


class Sink:
    address = "tcp://*:5558"

    def __init__(self, context: zmq.Context):
        self.socket = context.socket(zmq.PULL)
        self.socket.bind(self.address)

    def run(self):
        num_tasks = int(self.socket.recv())
        tstart = time.time()
        self.work(num_tasks)
        print(f"Elapsed time: {(time.time() - tstart) * 1000} msec")

    def work(self, num_tasks):
        for task in range(num_tasks):
            self.socket.recv()
            if task % 10 == 0:
                sys.stdout.write(':')
            else:
                sys.stdout.write('.')
            sys.stdout.flush()

    def shutdown(self):
        self.socket.close()


if __name__ == "__main__":
    context = zmq.Context()
    sink = Sink(context)

    atexit.register(context.destroy)
    atexit.register(sink.shutdown)

    run_with_debugger(sink.run)
