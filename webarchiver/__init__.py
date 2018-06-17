"""Decentralized web crawler and archiver."""
from webarchiver.config import *
from webarchiver.log import Log


def main(*args, **kwargs):
    """Performs checks and starts WebArchiver.

    Args:
        *args (list): Arguments.
        **kwargs (dict): Keyword arguments.
    """
    check()
    start(*args, **kwargs)


def start(sort, stager_host, stager_port, host, port):
    """Starts the log and WebArchiver.

    Args:
        sort (str): The type of the server. This can be ``'crawler'`` or
            ``'server'``.
        stager_host (str): The host of the stager server to connect to.
        stager_port (int): The port of the stager server to connect to.
        host (str): The host to use for this server.
        port (int): The port to use for this server.
    """
    log = Log()
    if sort == 'crawler':
        from webarchiver.server import CrawlerServer
        server = CrawlerServer(stager_host, stager_port, host, port)
    elif sort == 'stager':
        from webarchiver.server import StagerServer
        server = StagerServer(stager_host, stager_port, host, port)
    server.run()


def check():
    """Checks if everything is ready to start.

    Creates the ``CRAWL_DIRECTORY`` directory is it does not exist and checks
    if wget-lua is compiled.
    """
    if not os.path.isdir(LOGS_DIRECTORY):
        print('Directory \'{}\' not found, creating.'.format(LOGS_DIRECTORY))
        os.makedirs(LOGS_DIRECTORY)
    if not os.path.isdir(CRAWLS_DIRECTORY):
        print('Directory \'{}\' not found, creating.'.format(CRAWLS_DIRECTORY))
        os.makedirs(CRAWLS_DIRECTORY)
    if not os.path.isfile(WGET_LUA_FILENAME):
        print('File \'{0}\' not found. See the README for building {0}.'
              .format(WGET_LUA_FILENAME))
        sys.exit(1)

