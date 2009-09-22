#!/usr/bin/env python
START_PORT = 8800

import os
from subprocess import Popen
import sys

from twisted.spread import pb, flavors
from twisted.internet import reactor, defer

from filters.xml import XmlFilter, XslFilter

class Worker(object):
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.pid = -1
        self.status = 'idle'
        self.remote_object = None

    def create(self, worker_program):
        self.pid = Popen(["python", worker_program, str(self.port)])
        return self.pid
       
    def call_remote(self, _name, *args, **kw):
        """
        Convenience method for invoking the remote object
        """
        return self.remote_object.callRemote(_name, args, kw)

class WorkerGroup(object):
    def __init__(self, worker_program, start_port):
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
        return [w for w in self.workers if w.status == 'idle']

    def allocate_worker(self):
        for worker in self.free_workers:
            if worker.status == 'idle':
                worker.status = 'busy'
                return worker
        return None

class Controller(object):

    def __init__(self, tasks):
        self.tasks = tasks
        self.connected = False
        self.workers = WorkerGroup(start_port=START_PORT,
                worker_program='worker.py')
        reactor.callLater(1, self.connect) # Give the workers time to start

    # Utilities:
    def broadcastCommand(self, remoteMethodName, arguments, nextStep, failureMessage):
        "Send a command with arguments to all workers"
        print "broadcasting ...",
        deferreds = [worker.call_remote(remoteMethodName, arguments) for worker in self.workers.values()]
        print "broadcasted"
        reactor.callLater(3, self.check_status)
        # Use a barrier to wait for all to finish before nextStep:
        defer.DeferredList(deferreds, consumeErrors=True).addCallbacks(nextStep,
            self.failed, errbackArgs=(failureMessage))

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
            worker.remote_object.callRemote('run',
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

    def collectResults(self, results):
        print "step 3 results:", results

    @property
    def tasks_to_run(self):
        return [t for t in self.tasks if t.status == "created"] 

    @property
    def tasks_running(self):
        return [t for t in self.tasks if t.status == "running"] 

    def terminate(self):
        for worker in self.workers:
            worker.remote_object.callRemote("terminate").addErrback(self.failed,
                    "Termination Failed")
            reactor.stop

class FlexInterface(pb.Root):
    """
    Interface to Flex control panel (Make sure you have at least PyAMF 0.3.1)
    """
    def __init__(self, controller):
        self.controller = controller

    def start(self, _):
        self.controller.start()
        return "Starting parallel jobs"

    def terminate(self, _):
        for worker in controller.workers.values():
            worker.callRemote("terminate").addErrback(self.controller.failed, "Termination Failed")
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
