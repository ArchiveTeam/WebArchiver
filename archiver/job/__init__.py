import os
import string
import threading
import time

from archiver.config import *
from archiver.job.archive import ArchiveUrls
from archiver.utils import *

def initial_urls(url, list_):
    if url is None:
        return set()
    elif list_:
        return set(url)
    else:
        return set([url])


class Job(threading.Thread):
    def __init__(self, identifier, set_files, set_urls, set_found):
        threading.Thread.__init__(self)
        self._identifier = identifier
        self._directory = os.path.join(CRAWLS_DIRECTORY,
                                       self._identifier)
        self._urls = set()
        self._set_files = set_files
        self._set_urls = set_urls
        self._set_found = set_found
        self._last_time = 0
        self._last_time_url = 0
        self._crawls = []
        self.finished = False
        self.bad = False
        self._url_quota = 0

    def run(self):
        while not self.finished:
            if len(self._urls) > 0 \
                and self._url_quota >= CRAWLER_MIN_URL_QUOTA \
                and (check_time(self._last_time, JOB_MAX_WAIT)
                or check_time(self._last_time_url, JOB_MAX_WAIT_URLS)
                or len(self._urls) == JOB_MAX_URLS):
                self.run_crawl()
            time.sleep(1)

    def run_crawl(self):
        self._last_time = time.time()
        self._crawls.append(threading.Thread(target=self._new_crawl))
        self._crawls[-1].daemon = True
        self._crawls[-1].start()

    def increase_url_quota(self, quota):
        self._url_quota += quota

    def _new_crawl(self):
        quota = min(self._url_quota, len(self._urls))
        urls = {self._urls.pop() for i in range(quota)}
        self._url_quota -= quota
        self._urls.difference_update(urls)
        directory = self._directory + '_' + random_string(10)
        found = ArchiveUrls(directory, urls).run()
        if found is not False:
            for filename in os.listdir(directory):
                if filename.endswith('.warc.gz'):
                    self._set_files.add((self._identifier, os.path.join(directory, filename)))
            for url in urls:
                self._set_urls.add((self._identifier, url))
            for parenturl, url in found:
                self._set_found.add((self._identifier, parenturl, url))
        else:
            for url in urls:
                self._urls.add(url)
            # TODO remove crawl directory?

    def add_url(self, url):
        self._last_time_url = time.time()
        self._urls.add(url)

