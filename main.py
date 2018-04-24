import argparse
import os
import sys

from archiver.config import *


def main():
    if not os.path.isdir(CRAWLS_DIRECTORY):
        os.makedirs(CRAWLS_DIRECTORY)
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
        from archiver.server import CrawlerServer
        server = CrawlerServer(arguments.host, arguments.port,
                               arguments.stager_host, arguments.stager_port)
    elif arguments.sort == 'stager':
        from archiver.server import StagerServer
        server = StagerServer(arguments.host, arguments.port,
                              arguments.stager_host, arguments.stager_port)
    server.run()

if __name__ == '__main__':
    main()

