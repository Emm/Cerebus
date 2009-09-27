START_PORT = 8800

import os
from subprocess import Popen
import sys

from twisted.spread import pb, flavors
from twisted.internet import reactor, defer

class Worker(object):
    """
    Wrapper around a RemoteObject instance. 
    
    It has the following fields:
    - *address*: the host on which it is running
    - *port*: the port on which it is running
    - *pid*: its pid
    - *status*: its status - can be one of *idle* or *busy*
    - *remote_object*: the remote_object it wraps
    """

    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.pid = -1
        self.status = 'idle'
        self.remote_object = None

    def create(self, worker_program):
        """
        Starts a new worker process on *localhost*. Starts a new Python instance
        and runs *worker_program*.
        """
        self.pid = Popen(["python", worker_program, str(self.port)])
        return self.pid
       
    def call_remote(self, _name, *args, **kw):
        """
        Convenience method for invoking the remote object
        """
        return self.remote_object.callRemote(_name, *args, **kw)

class WorkerGroup(object):
    """
    A group of :class:`Worker` objects. It is responsible for creating
    individual workers and allocating them for a task.
    """

    def __init__(self, worker_program, start_port):
        """
        Creates a new worker group and instantiates one worker per CPU.
        """
        self.workers = []
        self.start_port = start_port
        self.worker_program = worker_program
        self._create_workers()

    def detect_cpus(self):
        """
        Detects the number of CPUs on a system. Cribbed from pp.
        """
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
            if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
                # Linux & Unix:
                ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                if isinstance(ncpus, int) and ncpus > 0:
                    return ncpus
            else: # OSX:
                return int(os.popen2("sysctl -n hw.ncpu")[1].read())
        # Windows:
        if os.environ.has_key("NUMBER_OF_PROCESSORS"):
                ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
                if ncpus > 0:
                    return ncpus
        return 1 # Default

    def _create_workers(self):
        """
        Creates the workers
        """
        cores = self.detect_cpus()
        # Start a subprocess on a core for each worker:
        for i in xrange(self.start_port, self.start_port + cores):
            worker = Worker('localhost', i)
            worker.create(self.worker_program)
            self.workers.append(worker)

    def __iter__(self):
        return iter(self.workers)

    def __getitem__(self, i):
        return self.workers[i]

    @property
    def free_workers(self):
        """
        Returns a list of free (idle) workers
        """
        return [w for w in self.workers if w.status == 'idle']

    def allocate_worker(self):
        """
        Allocates a worker from the pool. Returns the selected worker, or `None`
        if no worker was available.
        """
        for worker in self.free_workers:
            if worker.status == 'idle':
                worker.status = 'busy'
                return worker
        return None

class Controller(object):
    """
    Creates a worker group
    """
    def __init__(self, tasks):
        self.tasks = tasks
        self.connected = False
        self.workers = WorkerGroup(start_port=START_PORT,
                worker_program='worker.py')
        reactor.callLater(1, self.connect) # Give the workers time to start

    def check_status(self):
        "Show that workers can still receive messages"
        for worker in self.workers.values():
            worker.call_remote("status").addCallbacks(lambda r: sys.stdout.write(r + "\n"),
                self.failed, errbackArgs=("Status Check Failed"))
        print "Status calls made"

    def failed(self, results, failureMessage="Call Failed"):
        for (success, returnValue), (address, port) in zip(results, self.workers):
            if not success:
                raise Exception("address: %s port: %d %s" % (address, port, failureMessage))

    def connect(self):
        "Begin the connection process"
        connections = []
        for worker in self.workers:
            factory = pb.PBClientFactory()
            reactor.connectTCP(worker.address, worker.port, factory)
            connections.append(factory.getRootObject())
        defer.DeferredList(connections, consumeErrors=True).addCallbacks(
            self.store_connections, self.failed, errbackArgs=("Failed to Connect"))

    def store_connections(self, results):
        for i, result in enumerate(results):
            success, remote_object = result
            self.workers[i].remote_object = remote_object
        print "Connected; self.workers:", self.workers
        self.connected = True

    def get_next_task(self):
        task = self.tasks_to_run[0]
        task.status = "running"
        return task

    def run_tasks(self):
        """Runs commands on all available workers"""

        tasks_left = self.tasks_to_run
        free_workers = self.workers.free_workers
        if len(tasks_left) == 0:
            print "No more tasks to run"
            if len(self.tasks_running) == 0:
                print "All tasks have been run!"
                self.terminate()
            return

        if len(free_workers) == 0:
            print "No free CPU"
            return

        command_count = min(len(tasks_left), len(free_workers))
        for i in xrange(0, command_count):
            worker = self.workers.allocate_worker() 
            if not worker:
                raise StandardError("Could not allocate a new worker")
            task = self.get_next_task()
            failure_message = "Error while processing %s on %s" % (task, worker)
            worker.call_remote('run',
                    task).addCallbacks(self.run_next_tasks,
                            callbackArgs=(worker, task),
                            errback=self.failed, errbackArgs=(failure_message))

    def run_next_tasks(self, result, freed_worker, completed_task):
        freed_worker.status = 'idle'
        completed_task.status = 'completed'
        self.run_tasks()

    def start(self):
        "Begin the solving process"
        if not self.connected:
            return reactor.callLater(0.5, self.start)
        self.run_tasks()

    @property
    def tasks_to_run(self):
        return [t for t in self.tasks if t.status == "created"] 

    @property
    def tasks_running(self):
        return [t for t in self.tasks if t.status == "running"] 

    def terminate(self):
        for worker in self.workers:
            worker.call_remote("terminate").addErrback(self.failed,
                    "Termination Failed")
        reactor.callLater(1, reactor.stop)
        return "Terminating remote workers"

if __name__ == "__main__":
    tasks = []
    tasks.append(XmlFilter())
    tasks.append(XslFilter())
    tasks.append(XmlFilter())
    controller = Controller(tasks)
    # Tell the twisted reactor to listen:
    reactor.callLater(2, controller.start)
    # One reactor runs all servers and clients:
    reactor.run()
