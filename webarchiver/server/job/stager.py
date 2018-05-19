import time

from webarchiver.config import *
from webarchiver.server.base import Node
from webarchiver.url import init_urls
from webarchiver.utils import sample, split_set


class StagerServerJob:
    def __init__(self, settings, initial, initial_stager=None):
        self.settings = settings
        self.initial = initial
        self.initial_stager = initial_stager
        self.discovered_urls = init_urls(self.identifier, self.initial_urls) \
            if self.initial else {}
        self.current_urls = {}
        self.finished = False

        self.crawlers = {}
        self.stager = {}
        self.backup = {}

    def add_crawler(self, s):
        if s in self.crawlers:
            return None
        self.crawlers[s] = StagerServerJobCrawler(s)

    def crawler_confirmed(self, s):
        self.crawlers[s].confirmed = True

    def add_stager(self, s):
        self.stager[s] = StagerServerJobStager(s)
        self.backup[s.listener] = {}

    def backup_url(self, s, urlconfig):
        self.backup[s.listener][urlconfig.url] = urlconfig

    def share_urls(self):
        if len(self.discovered_urls) == 0:
            return None
        url_lists = split_set(self.discovered_urls, len(self.stager)+1)
        backups = sample(self.stager, MAX_BACKUPS)
        for urlconfig in url_lists.pop():
            #self.add_url_crawler(url)
            yield urlconfig, None, backups
            del self.discovered_urls[urlconfig.url]
        for s in self.stager:
            backups = sample(['this'] + [s_ for s_ in self.stager if s_ != s],
                             MAX_BACKUPS) # FIXME make pretty
            add_current = 'this' in backups
            if add_current:
                print('backup to self')
                backups.remove('this')
            for urlconfig in url_lists.pop():
                yield urlconfig, s, backups
                if add_current:
                    self.backup_url(s, urlconfig)
                del self.discovered_urls[urlconfig.url]

    def add_url_crawler(self, urlconfig):
        crawler = sample(self.crawlers, 1)[0]
        self.current_urls[urlconfig.url] = urlconfig
        self.crawlers[crawler].add_url(urlconfig.url)
        return crawler

    def add_url(self, urlconfig):
        self.discovered_urls[urlconfig.url] = urlconfig

    def finish_url(self, s, url, listener):
        if listener in self.backup:
            #job['urls'].remove(url) #TODO should this be currently crawling or currently using
            del self.backup[listener][url]
        else:
            print(self.current_urls)
            del self.current_urls[url]
            self.crawlers[s].remove_url(url)

    def confirmed(self, s, i):
        if  self.stager[s].confirmed:
            return None
        self.stager[s].confirmed = True
        if i == 0:
            return 1
        elif i == -1:
            return -1 # TODO  waiting for each s to add job?

    def started_stager(self, s):
        self.stager[s].started = True

    def started_crawl(self, s):
        self.crawlers[s].started = True

    def add_counter(self, s):
        self.counter = s

    def set_as_counter(self):
        self.counter = time.time()

    @property
    def rate(self):
        return self.settings.rate

    @property
    def identifier(self):
        return self.settings.identifier

    @property
    def initial_urls(self):
        if self.initial:
            return self.settings.urls
        return ()

    @property
    def started(self):
        for c in self.stager.values():
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
    def stager_confirmed(self):
        for c in self.stager.values():
            if not c.confirmed:
                return False
        return True

    @property
    def url_quota(self):
        assert self.is_counter
        old = self.counter
        self.counter = time.time()
        return int((self.counter-old)*self.rate)

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


class StagerServerJobCrawler:
    def __init__(self, s):
        self.s = s
        self.confirmed = False
        self.started = False
        self.urls = set()

    def add_url(self, url):
        self.urls.add(url)

    def remove_url(self, url):
        self.urls.remove(url)


class StagerServerJobStager:
    def __init__(self, s):
        self.s = s
        self.confirmed = False
        self.started = False

__all__ = (StagerServerJob,)

