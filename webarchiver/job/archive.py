import os
import shutil
import subprocess
import time

from webarchiver.config import *
from webarchiver.utils import sha512_file
from webarchiver.warc import WarcFile


class ArchiveUrls:
    def __init__(self, directory, urls=None):
        self.urls = urls
        self.directory = directory
        self._found_urls_path = os.path.join(directory, FOUND_URLS_FILE)
        if not os.path.isdir(self.directory):
            os.makedirs(self.directory)

    def run(self):
        wget_lua_return_code = self.archive()
        print('code', wget_lua_return_code)
        if wget_lua_return_code not in WGET_LUA_RETURN_CODES:
            return False
        self.warc_file.deduplicate()
        with open(self._found_urls_path, 'r') as f:
            return set([tuple(s.strip().split('\0'))
                        for s in f.read().splitlines() if len(s) > 0])

    def archive(self):
        os.environ['FOUND_URLS_FILE'] = self._found_urls_path
        return subprocess.call(self.arguments)

    @property
    def warc_file(self):
        if not hasattr(self, '_warc_file'):
            self._warc_file = WarcFile(self.directory)
        return self._warc_file

    @property
    def arguments(self):
        if not hasattr(self, '_arguments'):
            arguments = [
                WGET_LUA_FILENAME,
                '--user-agent', USER_AGENT,
                '--no-verbose',
                '--no-cookies',
                '--lua-script', LUA_SCRIPT_PATH,
                '--no-check-certificate',
                '--output-file', os.path.join(self.directory, WGET_LOG),
                '--output-document', os.path.join(self.directory, WGET_TEMP),
                '--execute', 'robots=off',
                '--rotate-dns',
                '--no-parent',
                '--page-requisites',
                '--timeout', WGET_TIMEOUT,
                '--tries', 'inf',
                '--span-hosts',
                '--waitretry', WGET_WAITRETRY,
                '--warc-file', self.warc_file.pathname.replace('.warc.gz', ''),
                '--warc-header', 'operator: Archive Team',
                '--warc-header', 'archiver-version: {}'.format(VERSION),
            ]
            for url in self.urls:
                arguments.extend([
                    '--warc-header', 'source-url: {}'.format(url),
                ])
            for filename in FILES:
                arguments.extend([
                    '--warc-header',
                    'hash-{}: {}'.format(filename, sha512_file(filename))
                ])
            for url in self.urls:
                arguments.append(url)                          
            self._arguments = arguments
        return self._arguments

