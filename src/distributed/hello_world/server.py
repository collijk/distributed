#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#
import atexit
import random
import time

import zmq

from distributed.utilities import run_with_debugger


class Server:
    address = "tcp://*:5556"

    def __init__(self, context: zmq.Context):
        self.socket = context.socket(zmq.REP)
        self.socket.bind(self.address)

    def run(self):
        while True:
            message = self.socket.recv().decode()
            reply = self.work(message)
            self.socket.send(reply)

    def work(self, message):
        print(f"Received request: {message}")
        print("Processing...")
        time.sleep(random.uniform(0.1, 0.3))
        return b"World"

    def shutdown(self):
        self.socket.close()


if __name__ == "__main__":
    context = zmq.Context()
    server = Server(context)

    atexit.register(context.destroy)
    atexit.register(server.shutdown)

    run_with_debugger(server.run)
