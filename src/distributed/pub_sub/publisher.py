#
#   Weather update server
#   Binds PUB socket to tcp://*:5556
#   Publishes random weather updates
#
import atexit
import random

import zmq

from distributed.utilities import run_with_debugger

class Publisher:
    address = "tcp://*:5556"

    def __init__(self, context: zmq.Context):
        self.socket = context.socket(zmq.PUB)
        self.socket.bind(self.address)

    def run(self):
        while True:
            message = self.work()
            self.socket.send_string(message)

    def work(self):
        zipcode = random.randrange(1, 100000)
        temperature = random.randrange(-80, 135)
        relhumidity = random.randrange(10, 60)
        return f"{zipcode} {temperature} {relhumidity}"

    def shutdown(self):
        self.socket.close()


if __name__ == "__main__":
    context = zmq.Context()
    publisher = Publisher(context)

    atexit.register(context.destroy)
    atexit.register(publisher.shutdown)

    run_with_debugger(publisher.run)
