# Poller
#
# Polls multiple connections to do work.
#
import atexit
import sys
import time

import zmq

from distributed.utilities import run_with_debugger


class DumbPoller:
    pull_address = "tcp://localhost:5557"
    sub_address = "tcp://localhost:5556"

    def __init__(self, context: zmq.Context):
        self.pull_socket = context.socket(zmq.PULL)
        self.pull_socket.connect(self.pull_address)

        self.sub_socket = context.socket(zmq.SUB)
        self.sub_socket.connect(self.sub_address)
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b"10001")

    def run(self):
        while True:
            while True:
                try:
                    msg = self.pull_socket.recv(zmq.DONTWAIT)
                except zmq.Again:
                    break
                self.work('pull', msg)

            while True:
                try:
                    msg = self.sub_socket.recv_string(zmq.DONTWAIT)
                except zmq.Again:
                    break
                self.work('sub', msg)

            time.sleep(0.001)

    def work(self, work_type, msg):
        if work_type == 'pull':
            result = int(msg) * 0.001
            print(f'Working: {result}')
            time.sleep(result)

        if work_type == "sub":
            _, temperature, _ = msg.split()
            print(f"Zip {10001} received tempearture {float(temperature)}")

    def shutdown(self):
        self.pull_socket.close()
        self.sub_socket.close()


class Poller:
    pull_address = "tcp://localhost:5557"
    sub_address = "tcp://localhost:5556"

    def __init__(self, context: zmq.Context):
        self.pull_socket = context.socket(zmq.PULL)
        self.pull_socket.connect(self.pull_address)

        self.sub_socket = context.socket(zmq.SUB)
        self.sub_socket.connect(self.sub_address)
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b"10001")

        self.poller = zmq.Poller()
        self.poller.register(self.pull_socket, zmq.POLLIN)
        self.poller.register(self.sub_socket, zmq.POLLIN)

    def run(self):
        while True:
            socks = dict(self.poller.poll())

            if self.pull_socket in socks:
                msg = self.pull_socket.recv()
                self.work('pull', msg)

            if self.sub_socket in socks:
                msg = self.sub_socket.recv_string(zmq.DONTWAIT)
                self.work('sub', msg)

    def work(self, work_type, msg):
        if work_type == 'pull':
            result = int(msg) * 0.001
            print(f'Working: {result}')
            time.sleep(result)

        if work_type == "sub":
            _, temperature, _ = msg.split()
            print(f"Zip {10001} received tempearture {float(temperature)}")

    def shutdown(self):
        self.pull_socket.close()
        self.sub_socket.close()


if __name__ == "__main__":
    context = zmq.Context()

    if len(sys.argv) > 1:
        poller = DumbPoller(context)
    else:
        poller = Poller(context)

    atexit.register(context.destroy)
    atexit.register(poller.shutdown)

    run_with_debugger(poller.run)
