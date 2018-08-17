#
# Task ventilator
# Binds PUSH socket to tcp://localhost:5557
# Sends batch of tasks to workers via that socket
#
import atexit
import random
import time

import zmq

from distributed.utilities import run_with_debugger


class Ventilator:
    address = "tcp://*:5557"
    sink_address = "tcp://localhost:5558"

    def __init__(self, context: zmq.Context, tasks_to_send: int):
        self.tasks_to_send = tasks_to_send

        self.socket = context.socket(zmq.PUSH)
        self.socket.bind(self.address)

        self.sink_socket = context.socket(zmq.PUSH)
        self.sink_socket.connect(self.sink_address)

    def run(self):
        print("Press Enter when the workers are ready: ")
        _ = input()
        print("Sending tasks to workers…")
        # The first message is "0" and signals start of batch
        self.sink_socket.send(b'0')

        self.work()

    def work(self):
        # Initialize random number generator
        random.seed()
        total_msec = 0
        for task_nbr in range(self.tasks_to_send):
            # Random workload from 1 to 100 msecs
            workload = random.randint(1, 100)
            total_msec += workload
            self.socket.send_string(f'{workload}')

        print(f"Total expected cost: {total_msec} msec")

    def shutdown(self):
        self.socket.close()
        

if __name__ == "__main__":
    context = zmq.Context()
    publisher = Publisher(context)

    atexit.register(context.destroy)
    atexit.register(publisher.shutdown)

    run_with_debugger(publisher.run)
