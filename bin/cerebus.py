#!/usr/bin/env python

"""
Cerebus executable. Parses options and triggers the loading of the
configuration, the chain description file and starts the processing.
"""

from copy import copy
from optparse import OptionParser, Option, OptionValueError
import os
import os.path
import sys

from runner import main

DEFAULT_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'conf', 'env.cfg')

def check_file(option, opt, value):
    """
    Checks the value of an option of type 'file' for existence and tests if it
    is a file
    """
    if not os.path.exists(value):
        raise OptionValueError(
            "option %s: file missing: %s" % (opt, value))
    if not os.path.isfile(value):
        raise OptionValueError(
            "option %s: this is not a file: %s" % (opt, value))
    if not os.access(value, os.R_OK):
        raise OptionValueError(
            "option %s: cannot read file: %s" % (opt, value))
    return value

class MyOption(Option):
    """
    Custom ConfigParser option class
    """
    TYPES = Option.TYPES + ("file",)
    TYPE_CHECKER = copy(Option.TYPE_CHECKER)
    TYPE_CHECKER["file"] = check_file

if __name__ == "__main__":
    parser = OptionParser(usage="%s [options] chain_description_file" % __file__, option_class=MyOption)

    parser.add_option("-c", "--conf", dest="config_file", type="file",
            help="Loads the configuration FILE", metavar="FILE")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
            help="Sets the log level to DEBUG")

    (options, args) = parser.parse_args()

    if len(args) != 1:
        print parser.usage
        sys.exit(1)
    chain_file = args[0]

    if not os.path.exists(chain_file):
        raise IOError("The chain description file does not exist: %s" % \
                chain_file)
    if not os.path.isfile(chain_file):
        raise IOError("This is not a file: %s" % chain_file)
    if not os.access(chain_file, os.R_OK):
        raise IOError("The chain description file cannot be read: %s" % \
                chain_file)

    config_file = options.config_file or DEFAULT_CONFIG_FILE
    main.load(os.path.abspath(config_file), os.path.abspath(chain_file),
            options.debug)
    main.run()
