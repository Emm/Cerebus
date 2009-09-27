import sys, random, math
from twisted.spread import pb
from twisted.internet import reactor

# Import filters to get the flavors.setUnjellyableForClass cruft
import cerebus.filters

class Worker(pb.Root):
    """
    Worker process, launched by :class:`Worker`
    """
    def __init__(self, id):
        self.id = id

    def __str__(self):
        return "Worker %s" % self.id

    def remote_run(self, task):
        """
        Runs a given task
        """
        return task.run()

    def remote_status(self):
        return "%s operational" % self

    def remote_terminate(self):
        """
        Terminates the worker process
        """
        reactor.callLater(0.5, reactor.stop)
        return "%s terminating..." % self

if __name__ == "__main__":
    port = int(sys.argv[1])
    reactor.listenTCP(port, pb.PBServerFactory(Worker(sys.argv[1])))
    reactor.run()
