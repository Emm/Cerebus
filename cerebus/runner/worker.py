"""
solver.py
Solves one portion of a problem, in a separate process on a separate CPU
"""
import sys, random, math
from twisted.spread import pb
from twisted.internet import reactor

import filters.xml

class Worker(pb.Root):

    def __init__(self, id):
        self.id = id

    def __str__(self): # String representation
        return "Worker %s" % self.id

    def remote_initialize(self, initArg):
        return "%s initialized" % self

    def run(self, task):
        result = 0
        return task.run()

    # Alias methods, for demonstration version:
    remote_run = run

    def remote_status(self):
        return "%s operational" % self

    def remote_terminate(self):
        reactor.callLater(0.5, reactor.stop)
        return "%s terminating..." % self

if __name__ == "__main__":
    port = int(sys.argv[1])
    reactor.listenTCP(port, pb.PBServerFactory(Worker(sys.argv[1])))
    reactor.run()
