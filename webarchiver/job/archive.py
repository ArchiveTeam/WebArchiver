"""Archives data from the internet."""
import os
import shutil
import subprocess
import time

from webarchiver.config import *
from webarchiver.utils import sha512_file
from webarchiver.warc import WarcFile


class ArchiveUrls:
    """Archives URLs.

    Attributes:
        urls (list of str): List of URLs to archive.
        directory (str): Directory where the files from the crawl are stored.
    """

    def __init__(self, directory, urls):
        """Inits the archival of URLs.

        If the directory for the fiels from the crawl does not exist it will be
        created.

        Args:
            urls (list of str): List of URLs to archive.
            directory (str): Directory where the files from the crawl are
                stored.
        """
        self.urls = urls
        self.directory = directory
        self._found_urls_path = os.path.join(directory, FOUND_URLS_FILE)
        if not os.path.isdir(self.directory):
            os.makedirs(self.directory)

    def run(self):
        """Runs the crawl.

        An archive jobs is started and the return code is checked against the
        allowed list of return codes. The resulting WARC file is dedupicated
        and discovered URLs are loaded is they are readable URLs. The extracted
        data about URLs was merged using ``\\0``, this is split again. The data
        is::

            <parent URL> <discovered URL>

        Returns:
            set of tuples: Each tuple consists of the parent URL and discovered
                URL.
            bool: If the return code from the crawl is not in the list of
                allowed return codes.
        """
        wget_lua_return_code = self.archive()
        print('code', wget_lua_return_code)
        if wget_lua_return_code not in WGET_LUA_RETURN_CODES:
            return False
        self.warc_file.deduplicate()
        discovered_data = set()
        with open(self._found_urls_path, 'rb') as f:
            for line in f:
                if len(line) == 0:
                    continue
                try:
                    line = line.decode('utf-8')
                except UnicodeDecodeError:
                    continue
                discovered_data.add(tuple(line.strip().split('\0')))
        return discovered_data

    def archive(self):
        """Runs a crawl job.

        Set the environment variables for the crawl and run the crawl.

        Returns:
            int: The return code of the crawl.
        """
        os.environ['FOUND_URLS_FILE'] = self._found_urls_path
        return subprocess.call(self.arguments)

    @property
    def warc_file(self):
        """:obj:`webarchiver.warc.WarcFile`: The WARC file object."""
        if not hasattr(self, '_warc_file'):
            self._warc_file = WarcFile(self.directory)
        return self._warc_file

    @property
    def arguments(self):
        """list: Argument for the crawl.""" #TODO extend doc with options in list
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

