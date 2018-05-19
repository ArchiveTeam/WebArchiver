import re
import time

from webarchiver.database import UrlDeduplicationDatabase
from webarchiver.job import Job


class CrawlerServerJob:
    def __init__(self, settings, filenames_set, finished_urls_set,
                 found_urls_set):
        self.settings = settings
        self.stager = []
        self.started = False
        self.received_url_quota = time.time()
        self._job = Job(self.identifier, filenames_set, finished_urls_set,
                        found_urls_set)
        self._urls = {}
        self._url_database = UrlDeduplicationDatabase(self.identifier,
            'crawler_' + self.identifier)

    def add_stager(self, s):
        if s in self.stager:
            return None
        self.stager.append(s)

    def add_url(self, s, urlconfig):
        if self.archived_url(urlconfig):
            return None
        self._urls[urlconfig] = s
        self._job.add_url(urlconfig)

    def increase_url_quota(self, i):
        self._received_url_quota = time.time()
        self._job.increase_url_quota(i)

    def get_url_stager(self, urlconfig):
        return self._urls[urlconfig]

    def delete_url_stager(self, urlconfig):
        del self._urls[urlconfig]

    def start(self):
        if self.is_started:
            return None
        self._job.start()
        self.started = True
        return True

    def finished_url(self, urlconfig):
        self._url_database.insert(urlconfig)

    def archived_url(self, urlconfig):
        return self._url_database.has_url(urlconfig.url)

    def allowed_url(self, urlconfig):
        for regex in self.settings.regex:
            if re.search(regex, urlconfig.url):
                break
        else:
            return False
        if urlconfig.depth > self.max_depth:
            return False
        return True

    @property
    def max_depth(self):
        return self.settings.depth

    @property
    def is_started(self):
        return self.started or self._job.ident

    @property
    def identifier(self):
        return self.settings.identifier

