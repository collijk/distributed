#
#   Weather update client
#   Connects SUB socket to tcp://localhost:5556
#   Collects weather updates and finds avg temp in zipcode
#
import atexit
import sys

import zmq

from distributed.utilities import run_with_debugger


class Subscriber:
    address = "tcp://localhost:5556"

    def __init__(self, context: zmq.Context, zipcode):
        self.socket = context.socket(zmq.SUB)
        print("Collecting updates from weather server…")
        self.socket.connect(self.address)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, zipcode)

        self.zipcode = zipcode
        self.total_temperature = 0

    def run(self):
        # Process 5 updates
        updates = 5
        for update_nbr in range(updates):
            message = self.socket.recv_string()
            self.work(message)

        print(f"Average temperature for zipcode '{self.zipcode}' was {self.total_temperature/updates}")

    def work(self, message):
        _, temperature, _ = message.split()
        print("")
        self.total_temperature += float(temperature)

    def shutdown(self):
        self.socket.close()


if __name__ == "__main__":
    context = zmq.Context()
    server = Subscriber(context, sys.argv[1])

    atexit.register(context.destroy)
    atexit.register(server.shutdown)

    run_with_debugger(server.run)





