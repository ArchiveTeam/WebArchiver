"""Decentralized web crawler and archiver."""
import atexit
import distutils.spawn
import logging
import sys
import threading

from webarchiver.config import *
from webarchiver import dashboard
from webarchiver.log import Log

if not os.path.isdir(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY)
log = Log()

logger = logging.getLogger(__name__)


def main(*args, **kwargs):
    """Performs checks and starts WebArchiver.

    Args:
        *args (list): Arguments.
        **kwargs (dict): Keyword arguments.
    """
    check()
    logger.info('Starting WebArchiver.')
    start(*args, **kwargs)


def version():
    print(VERSION)


def start(sort, stager_host, stager_port, host, port, no_dashboard,
          dashboard_port):
    """Starts the log and WebArchiver.

    Args:
        sort (str): The type of the server. This can be ``'crawler'`` or
            ``'server'``.
        stager_host (str): The host of the stager server to connect to.
        stager_port (int): The port of the stager server to connect to.
        host (str): The host to use for this server.
        port (int): The port to use for this server.
    """
    if sort == 'crawler':
        logger.info('Starting crawler server.')
        from webarchiver.server import CrawlerServer
        server = CrawlerServer(stager_host, stager_port, host, port)
    elif sort == 'stager':
        logger.info('Starting stager server.')
        from webarchiver.server import StagerServer
        server = StagerServer(stager_host, stager_port, host, port)
    if not no_dashboard:
        dashboard.create(dashboard_port, server)
    server.run()


def check():
    """Checks if everything is ready to start.

    Creates the ``CRAWL_DIRECTORY`` and ``LOGS_DIRECTORY`` directories if they
    do not exist and checks if wget-lua is compiled.
    """
    if not os.path.isdir(CRAWLS_DIRECTORY):
        logger.info('Directory \'%s\' not found, creating.', CRAWLS_DIRECTORY)
        os.makedirs(CRAWLS_DIRECTORY)
    if not distutils.spawn.find_executable(WGET_EXECUTABLE):
        logger.error('Executable \'%s\' not found, please install this.',
                     WGET_EXECUTABLE)
        sys.exit(1)

@atexit.register
def shutdown():
    log.shutdown()

