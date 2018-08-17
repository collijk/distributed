#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#
import atexit
import random
import sys
import time

import zmq

from distributed.utilities import run_with_debugger


class Client:
    address = "tcp://localhost:5556"

    def __init__(self, context, identity):
        self.id = identity
        self.socket = context.socket(zmq.REQ)

        print("Connecting to hello world server…")
        self.socket.connect(self.address)

    def run(self):
        for request in range(20):
            print(f"Sending request {request} …")
            self.socket.send(bytes(f"Hello from {self.id}", encoding='ascii'))

            message = self.socket.recv().decode()
            self.work(request, message)

    def work(self, request, message):
        print(f"Received reply {request} [ {message} ]")
        print("Processing...")
        time.sleep(random.random())

    def shutdown(self):
        self.socket.close()


if __name__ == "__main__":
    context = zmq.Context()
    client = Client(context, sys.argv[1])

    atexit.register(context.destroy)
    atexit.register(client.shutdown)

    run_with_debugger(client.run)
