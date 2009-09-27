import os
import os.path
import sys

from argparse import ArgumentParser

from cerebus.runner import runners

class Command(object):
    """
    Abstract command - a command is an action which can be executed by
    `Cerebus`'s command-line executable, `cerebus.py`
    """
    def __init__(self):
        self.parser = ArgumentParser(prog="%s %s" % \
                (os.path.basename(sys.argv[0]), self.name), add_help=False)
        self.parser.description = self.description.capitalize() + '.'

    @property
    def name(self):
        return self.__class__.__name__.lower()

    def parse_args(self, args):
        """
        Parses the arguments after `[command_name]` and executes the associated
        action.
        """
        own_args = self.parser.parse_args(args.command_arg) 
        self.run(own_args)

    def run(self, own_args):
        raise NotImplementedError()

def directory(path):
    """
    Adds a *directory* type to `argparse`'s parser
    """
    if not os.path.exists(path):
        raise TypeError("%s does not exist" % path)
    if not os.path.isdir(path):
        raise TypeError("%s is not a directory" % path)
    return path

#-----------------------------------
# Actual commands start HERE
#-----------------------------------
class Bootstrap(Command):
    description = "bootstraps a data processing chain"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_argument("location", help=("copies the files in the "
            "templates directory to LOCATION"), type=directory)

    def run(self, own_args):
        runners.Bootstrap(own_args.location).run()

class Run(Command):
    description = "runs a data processing chain"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_argument("chain_dir", help=("directory of a data "
                "processing chain to run"), type=directory)

    def run(self, own_args):
        runners.Processor(own_args.chain_dir).run()

class Help(Command):
    description = "provides help about other commands"

    def __init__(self, command_list):
        Command.__init__(self)
        self.command_list = command_list

    def parse_args(self, args):
        # This is a hack - we only add the argument to the parser now because
        # the list is not complete before
        self.parser.add_argument("command", choices=sorted(self.command_list.keys()))

        own_args = self.parser.parse_args(args.command_arg)
        command = self.command_list[own_args.command]
        print command.parser.print_help()
