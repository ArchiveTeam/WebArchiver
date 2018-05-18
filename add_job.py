import configparser
import os
import pickle
import sys

from webarchiver.config import *
from webarchiver.dicts import MultiDict
from webarchiver.job.settings import JobSettings


def main():
    if len(sys.argv) != 2:
        print('The config file is needed as argument.')
        exit(1)
    if not os.path.isfile(sys.argv[1]):
        print('File {} does not exist.'.format(sys.argv[1]))
        exit(1)
    if not os.path.isdir(NEW_JOBS_DIR):
        os.makedirs(NEW_JOBS_DIR)
    parser = configparser.RawConfigParser(dict_type=MultiDict, strict=False)
    parser.read(sys.argv[1])
    assert len(parser.sections()) == 1
    settings = JobSettings(parser.sections()[0],
                           dict(parser[parser.sections()[0]]),
                           sys.argv[1])
    outname = os.path.join(NEW_JOBS_DIR, '{}.pkl'.format(settings.identifier))
    with open(outname + '.dumping', 'wb') as f:
        pickle.dump(settings, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.rename(outname + '.dumping', outname)
    print('Found {} URLs.'.format(len(settings.urls)))
    print('Create job file in {}.'.format(outname))
    print('Exiting.')

if __name__ == '__main__':
    main()

