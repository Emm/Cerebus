#!/usr/bin/env python

"""
Cerebus executable. Can execute various actions, including:
    - **bootstrap**
    - **run**
    - **help**
"""

from argparse import ArgumentParser
import os.path
import sys


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import cerebus.commands as cc


def make_arg_parser(commands):
    """
    Creates and returns the argument parser
    """
    parser = ArgumentParser()
    parser.add_argument("command", help="Command to execute",
            choices=sorted(commands.keys()))
    parser.add_argument("command_arg", help="Argument to a command", nargs="*")
    parser.add_argument("-d", "--debug", dest="debug", action="store_true",
            help="Sets the log level to DEBUG - for now, doesn't have any effect")
    return parser

if __name__ == "__main__":
    commands = {}
    for command in (cc.Bootstrap(), cc.Run(), cc.Help(commands)):
        commands[command.name] = command
        
    parser = make_arg_parser(commands)
    args = parser.parse_args()

    command = args.command
    cmd = commands[command]
    cmd.parse_args(args)
