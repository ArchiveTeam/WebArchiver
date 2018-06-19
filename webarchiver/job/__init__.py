"""Managing jobs to archive data from the internet."""
import logging
import pickle
import os
import string
import threading
import time

from webarchiver.config import *
from webarchiver.job.archive import ArchiveUrls
from webarchiver.url import UrlConfig
from webarchiver.utils import *

logger = logging.getLogger(__name__)


def new_jobs():
    """Loads new available jobs.

    Every new job configuration ``.pkl`` file in ``NEW_JOBS_DIR`` is picked up
    and loaded with :mod:`pickle`. After a file is loaded ``.loaded`` is
    appended to its filename.

    Yields:
        :obj:`webarchiver.job.settings.JobSettings`: The loaded job
            configurations from the ``.pkl`` files.
    """
    while True:
        if os.path.isdir(NEW_JOBS_DIR):
            for filename in os.listdir(NEW_JOBS_DIR):
                if not filename.endswith('.pkl'):
                    continue
                logger.debug('Found new .pkl file %s.', filename)
                filename_ = os.path.join(NEW_JOBS_DIR, filename)
                with open(filename_, 'rb') as f:
                    yield pickle.load(f)
                os.rename(filename_, filename_ + '.loaded')
        time.sleep(10)


class Job(threading.Thread):
    """Class for the configuration and crawl of a job on the stager server.

    Attributes:
        finished (bool): True if the crawl is finished and no more URLs are
            available. False by default.
    """

    def __init__(self, identifier, set_files, set_urls, set_found):
        """Inits the crawl job.

        Note:
            The job is a subclass of :class:`threading.Thread`.
        Args:
            identifier (str): The job identifier.
            set_files (set): The set to which files are added to be uploaded.
            set_urls (set): The set to which finished URLs are added.
            set_found (set): The set to which discovered URL are added.
        """
        threading.Thread.__init__(self)
        self._identifier = identifier
        self._directory = os.path.join(CRAWLS_DIRECTORY, self._identifier)
        self._urls = set()
        self._set_files = set_files
        self._set_urls = set_urls
        self._set_found = set_found
        self._last_time = 0
        self._last_time_url = 0
        self._crawls = []
        self.finished = False
        self._url_quota = 0
        logger.debug('Created archive job %s.', self)

    def run(self):
        """Runs a loop to start a crawl for the currently received URLs.

        A crawl if only started if at least one URL is queued for archival and
        a minimum quota ``CRAWLER_MIN_URL_QUOTA`` of URLs to be archived is
        set. Besides this there should be a minimum of JOB_MAX_URLS URLs 
        queued, a minimum of JOB_MAX_WAIT seconds since the last crawl or a
        minimum of JOB_MAX_WAIT_URLS seconds since the last URL was added.
        """
        while not self.finished:
            if len(self._urls) > 0 \
                and self._url_quota >= CRAWLER_MIN_URL_QUOTA \
                and (check_time(self._last_time, JOB_MAX_WAIT)
                or check_time(self._last_time_url, JOB_MAX_WAIT_URLS)
                or len(self._urls) >= JOB_MAX_URLS):
                self.run_crawl()
            time.sleep(1)

    def run_crawl(self):
        """Starts a thread for a new crawl with :func:`self._new_crawl`."""
        self._last_time = time.time()
        self._crawls.append(threading.Thread(target=self._new_crawl))
        self._crawls[-1].daemon = True
        self._crawls[-1].start()

    def increase_url_quota(self, quota):
        """Increases the URL quota.

        Args:
            quota (int): The number of URLs to add to the quota.
        """
        logger.debug('Increasing URL quota for archive job %s with %s.', self,
                     quota)
        self._url_quota += quota

    def _new_crawl(self):
        """Runs a crawl and handles the output.

        The current queued :class:`webarchiver.url.UrlConfig` objects are taken
        and extacted URLs are archived using
        :class:`webarchiver.job.archive.ArchiveUrls`. The resulting WARCs,
        finished URLs and discovered URLs are added to their sets. The
        discovered URLs have their parent URL set to the old URL and have their
        depth increased.
        """
        logger.debug('Starting new crawl for archive job %s.', self)
        quota = min(self._url_quota, len(self._urls))
        urls = {self._urls.pop() for i in range(quota)}
        urls_depths = {urlconfig.url: urlconfig.depth for urlconfig in urls} 
        self._url_quota -= quota
        self._urls.difference_update(urls)
        directory = self._directory + '_' + random_string(10)
        found = ArchiveUrls(directory,
                            {urlconfig.url for urlconfig in urls}).run()
        if found is not False:
            with self._set_files.lock:
                for filename in os.listdir(directory):
                    if filename.endswith('.warc.gz'):
                        self._set_files.add((self._identifier,
                                             os.path.join(directory,
                                                          filename)))
            with self._set_urls.lock:
                self._set_urls.update(urls)
            with self._set_found.lock:
                for parenturl, url in found:
                    self._set_found.add(
                        UrlConfig(self._identifier, url,
                                  urls_depths[parenturl]+1, parenturl)
                    )
        else:
            self._urls.update(urls)
            # TODO remove crawl directory?

    def add_url(self, urlconfig):
        """Queues an URL to be archived.

        Args:
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration for
                the to be queued URL.
        """
        logger.debug('Adding URL %s to archiver job %s.', urlconfig, self)
        self._last_time_url = time.time()
        self._urls.add(urlconfig)

    def __repr__(self):
        return '<{} at 0x{:x} directory={}>'.format(__name__, id(self),
                                                    self._identifier)

