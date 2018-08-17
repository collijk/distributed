#
# Task ventilator
# Binds PUSH socket to tcp://localhost:5557
# Sends batch of tasks to workers via that socket
#
import atexit
import random
import sys
import time

import zmq

from distributed.utilities import run_with_debugger


class Pusher:
    address = "tcp://*:5557"

    def __init__(self, context: zmq.Context):

        self.socket = context.socket(zmq.PUSH)
        self.socket.bind(self.address)

    def run(self):
        print("Press Enter when the workers are ready: ")
        _ = input()
        print("Sending tasks to workers…")
        self.work()

    def work(self):
        # Initialize random number generator
        random.seed()
        total_msec = 0
        while True:
            tasks_to_send = random.randint(1, 10)
            for task_nbr in range(tasks_to_send):
                # Random workload from 1 to 100 msecs
                workload = random.randint(1, 100)
                self.socket.send_string(f'{workload}')

            # wait a while before we send more tasks
            wait_time = random.randint(1, 20)
            time.sleep(wait_time)

    def shutdown(self):
        self.socket.close()


if __name__ == "__main__":
    context = zmq.Context()
    publisher = Pusher(context)

    atexit.register(context.destroy)
    atexit.register(publisher.shutdown)

    run_with_debugger(publisher.run)
