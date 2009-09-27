from ConfigParser import ConfigParser

import imp
import logging
import os
import os.path
import re
import shutil
import sys

from cerebus.runner import install_runner

class Runner(object):
    """
    Abstract runner - loads the environment. There can only be one runner active
    at the same time. It is available globally as `cerebus.runner.main`.
    """
    def __init__(self, chain_root_dir=''):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('logger')
        self.config_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(
            sys.argv[0]))), 'conf', 'env.cfg')
        self.chain_root_dir = chain_root_dir
        self.config = self._load_config()
        install_runner(self)

    def _load_config(self):
        """
        Loads *config_file* from the main Cerebus installation directory and
        from the data processing chain (if any).
        Loads the configuration files for the logger as well
        """
        # Load main config files
        defaults = {
                'root_dir': os.path.dirname(os.path.dirname(self.config_file)),
                'chain_root_dir': self.chain_root_dir,
                'conf_dir': os.path.dirname(self.config_file)
                }
        config = ConfigParser(defaults=defaults)
        self.logger.debug("Parsing configuration file %s" % self.config_file)
        config.readfp(open(self.config_file))
        chain_config_file = config.get('main', 'chain_config_file')
        if os.path.exists(chain_config_file):
            self.logger.debug("Parsing chain configuration file %s" % \
                    chain_config_file)
            config.read(chain_config_file)

        # Load logger config files
        # New defaults for the logging conf
        defaults = {
                'chain_logging_dir': config.get('logging', 'chain_logging_dir'),
                }
        config_dir = os.path.dirname(self.config_file)
        main_logging_config_file = os.path.join(config_dir, 'logging.cfg')
        if os.path.exists(main_logging_config_file):
            self.logger.debug("Loading main logging configuration %s" % \
                    main_logging_config_file)
            logging.config.fileConfig(main_logging_config_file, defaults)
        else:
            self.logger.warn('No main logging file found')

        chain_logging_config_file = config.get('main',
                'chain_logging_config_file')
        if os.path.exists(chain_logging_config_file):
            self.logger.debug("Loading chain logging configuration file" % \
                    chain_logging_config_file)
            logging.config.fileConfig(chain_logging_config_file, defaults)
        return config

    def run(self):
        raise NotImplementedError()

class Bootstrap(Runner):
    """
    Bootstraps a new data processing chain by copying the content of the
    `templates` directory to *chain_root_dir*.
    """

    def run(self):
        template_dir = self.config.get('main', 'template_dir')
        # Copy recursively all files and directories from the templates
        # directory
        for root, dirs, files in os.walk(template_dir):
            rel_path = root[len(template_dir):]
            if rel_path.startswith('/'):
                rel_path = rel_path[1:]
            dest_path = os.path.join(self.chain_root_dir, rel_path)
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
            for file in files:
                src_path = os.path.join(root, file)
                self.logger.debug("Copying %s to %s" % (src_path, dest_path))
                src_path = os.path.join(root, file)
                shutil.copy(src_path, dest_path)

class Processor(Runner):
    """
    Runs the data processing chain
    """
    def run(self):
        chain_file = self.config.get('main', 'chain_file')
        if not os.path.exists(chain_file):
            raise IOError("The chain description file does not exist: %s" % \
                    chain_file)
        if not os.path.isfile(chain_file):
            raise IOError("This is not a file: %s" % chain_file)
        if not os.access(chain_file, os.R_OK):
            raise IOError("The chain description file cannot be read: %s" % \
                    chain_file)
        self.scheduler = self._load_chain_file(chain_file)

    def _load_chain_file(self, chain_file):
        # Load the chain description file
        imp.load_source('chain', chain_file)
