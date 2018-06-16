"""Start WebArchiver."""
import argparse
import os
import sys

from webarchiver.config import *


def check():
    """Checks if everything is ready to start.

    Creates the ``CRAWL_DIRECTORY`` directory is it does not exist and checks
    if wget-lua is compiled.
    """
    if not os.path.isdir(CRAWLS_DIRECTORY):
        print('{} not found, creating.'.format(CRAWLS_DIRECTORY))
        os.makedirs(CRAWLS_DIRECTORY)
    if not os.path.isfile(WGET_LUA_FILENAME):
        print('{0} not found. See the README for building {0}.' \
              .format(WGET_LUA_FILENAME))
        sys.exit(1)


def main():
    """Parses the arguments and start the scripts.

    A :class:`webarchiver.server.CrawlerServer` or
    :class:`webarchiver.server.StagerServer` server will be created depending
    on the given arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--sort',
                        help='The sort of server to be created.',
                        choices=['crawler', 'stager'], required=True)
    parser.add_argument('-SH', '--stager-host',
                        help='The host of the stager to connect to. This '
                        'should not be set if this is the first stager.',
                        type=str, metavar='HOST')
    parser.add_argument('-SP', '--stager-port',
                        help='The port of the stager to connect to. This '
                        'should not be set if this is the first stager.',
                        type=int, metavar='PORT')
    parser.add_argument('-H', '--host', help='The host to use for '
                        'communication. If not set the scripts will try to '
                        'determine the host.', type=str)
    parser.add_argument('-P', '--port', help='The port to use for '
                        'communication. If not set a random port between 3000 '
                        'and 6000 will be chosen.', type=int)
    arguments = parser.parse_args(sys.argv[1:])
    if arguments.sort == 'crawler':
        from webarchiver.server import CrawlerServer
        server = CrawlerServer(arguments.stager_host, arguments.stager_port,
                               arguments.host, arguments.port)
    elif arguments.sort == 'stager':
        from webarchiver.server import StagerServer
        server = StagerServer(arguments.stager_host, arguments.stager_port,
                               arguments.host, arguments.port)
    server.run()

if __name__ == '__main__':
    check()
    main()

