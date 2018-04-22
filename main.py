import argparse
import os
import sys

from archiver.config import *


def main():
    if not os.path.isdir(CRAWLS_DIRECTORY):
        os.makedirs(CRAWLS_DIRECTORY)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-S', '--sort', help='Stager or crawler server',
                        choices=['crawler', 'stager'], required=True)
    parser.add_argument('-H', '--host',
                        help='Host of stager server to connect to', type=str)
    parser.add_argument('-P', '--port',
                        help='Port of stager server to connect to', type=int)
    arguments = parser.parse_args(sys.argv[1:])
    if arguments.sort == 'crawler':
        from archiver.server import CrawlerServer
        server = CrawlerServer(arguments.host, arguments.port)
    elif arguments.sort == 'stager':
        from archiver.server import StagerServer
        server = StagerServer(arguments.host, arguments.port)
    server.run()

if __name__ == '__main__':
    main()
