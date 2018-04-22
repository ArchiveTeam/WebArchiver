import time

from archiver.config import *
from archiver.server.base import Node
from archiver.utils import sample, split_set


class StagingServerJob:
    def __init__(self, identifier, initial_urls=set(), initial_staging=None):
        self.identifier = identifier
        self.initial_staging = initial_staging
        self.initial_urls = set(initial_urls)
        self.discovered_urls = set(initial_urls)
        self.current_urls = set()
        self.finished = False

        self.crawlers = {}
        self.staging = {}
        self.backup = {}

        self.url_rate = 2

    def add_crawler(self, s):
        if s in self.crawlers:
            return None
        self.crawlers[s] = StagingServerJobCrawler(s)

    def crawler_confirmed(self, s):
        self.crawlers[s].confirmed = True

    def add_staging(self, s):
        self.staging[s] = StagingServerJobStaging(s)
        self.backup[s.listener] = set()

    def backup_url(self, s, url):
        self.backup[s.listener].add(url)

    def share_urls(self):
        if len(self.discovered_urls) == 0:
            return None
        url_lists = split_set(self.discovered_urls, len(self.staging)+1)
        backups = sample(self.staging, MAX_BACKUPS)
        for url in url_lists.pop():
            #self.add_url_crawler(url)
            yield url, None, backups
            self.discovered_urls.remove(url)
        for s in self.staging:
            backups = sample(['this'] + [s_ for s_ in self.staging if s_ != s],
                             MAX_BACKUPS) # FIXME make pretty
            add_current = 'this' in backups
            if add_current:
                print('backup to self')
                backups.remove('this')
            for url in url_lists.pop():
                yield url, s, backups
                if add_current:
                    self.backup_url(s, url)
                self.discovered_urls.remove(url)

    def add_url_crawler(self, url):
        crawler = sample(self.crawlers, 1)[0]
        self.current_urls.add(url)
        self.crawlers[crawler].add_url(url)
        return crawler

    def add_url(self, url):
        self.discovered_urls.add(url)

    def finish_url(self, s, url, listener):
        if listener in self.backup:
            #job['urls'].remove(url) #TODO should this be currently crawling or currently using
            self.backup[listener].discard(url)
        else:
            print(self.current_urls)
            self.current_urls.remove(url)
            self.crawlers[s].urls.remove(url)

    def confirmed(self, s, i):
        if  self.staging[s].confirmed:
            return None
        self.staging[s].confirmed = True
        if i == 0:
            return 1
        elif i == -1:
            return -1 # TODO  waiting for each s to add job?

    def started_staging(self, s):
        self.staging[s].started = True

    def started_crawl(self, s):
        self.crawlers[s].started = True

    def add_counter(self, s):
        self.counter = s

    def set_as_counter(self):
        self.counter = time.time()

    @property
    def started(self):
        for c in self.staging.values():
            if not c.started:
                return False
        return True

    @property
    def started_local_crawl(self):
        for c in self.crawlers.values():
            if not c.started:
                return False
        return True

    @property
    def staging_confirmed(self):
        for c in self.staging.values():
            if not c.confirmed:
                return False
        return True

    @property
    def url_quota(self):
        assert self.is_counter
        old = self.counter
        self.counter = time.time()
        return int((self.counter-old)*self.url_rate)

    @property
    def is_counter(self):
        return type(self.counter) == float

    @property
    def counter(self):
        if not hasattr(self, '_counter'):
            return None
        return self._counter

    @counter.setter
    def counter(self, value):
        assert type(value) in {float, Node}
        self._counter = value


class StagingServerJobCrawler:
    def __init__(self, s):
        self.s = s
        self.confirmed = False
        self.started = False
        self.urls = set()

    def add_url(self, url):
        self.urls.add(url)


class StagingServerJobStaging:
    def __init__(self, s):
        self.s = s
        self.confirmed = False
        self.started = False

__all__ = (StagingServerJob,)

