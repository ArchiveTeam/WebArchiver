"""Start WebArchiver using command line arguments."""
import argparse
import os
import sys

from webarchiver import main


def main_():
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
    main(arguments.sort, arguments.stager_host, arguments.stager_port,
         arguments.host, arguments.port)

if __name__ == '__main__':
    main_()

