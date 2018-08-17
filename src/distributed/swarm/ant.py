import argparse
import atexit
import sys
import time

import zmq

from distributed.utilities import run_with_debugger


class Ant:
    pull_address = "tcp://localhost:5557"
    push_address = "tcp://localhost:5558"

    def __init__(self, context: zmq.Context):
        self.receiver = context.socket(zmq.PULL)
        self.receiver.connect(self.pull_address)

        self.sender = context.socket(zmq.PUSH)
        self.sender.connect(self.push_address)

    def run(self):

        while True:
            work = self.receiver.recv()
            result = self.work(work)
            self.sender.send(b'.')

    def work(self, work):
        sys.stdout.write('.')
        sys.stdout.flush()
        result = int(work)*0.001
        time.sleep(int(work)*0.001)
        return result

    def shutdown(self):
        self.receiver.close()
        self.sender.close()


if __name__ == "__main__":
    context = zmq.Context()
    worker = Worker(context)

    atexit.register(context.destroy)
    atexit.register(worker.shutdown)

    run_with_debugger(worker.run)
