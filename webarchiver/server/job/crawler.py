"""Configuration for a job on a crawler server."""
import logging
import re
import time

from webarchiver.database import UrlDeduplicationDatabase
from webarchiver.job import Job

logger = logging.getLogger(__name__)


class CrawlerServerJob:
    """This holds the configuration for a job on a crawler server.

    The crawler server communicates with this to change configurations and add
    URLs for the job. This holds the configuration and crawl of the job, like
    the stager server connected to the job.

    Attributes:
        settings (:obj:`webarchiver.job.settings.JobSettings`): The settings
            for the job.
        stager (list of :obj:`webarchiver.server.Node`): The stager servers
            connected to the job.
        started (bool): Whether the job has started on the crawler server.
        received_url_quota (int): The last time in second the URL quota was
            received.
    """

    def __init__(self, settings, filenames_set, finished_urls_set,
                 found_urls_set):
        """Inits the job for the crawler server.

        A crawling job is created for the actual crawl and the database is for
        the URLs is started.

        Args:
            settings (:obj:`webarchiver.job.settings.JobSettings`): The
                settings for the job.
            filenames_set (set): The set to add the finished WARCs to.
            finished_urls_set (set): The set to add the finished URLs to.
            found_urls_set (set): The set to add the discovered URLs to.
        """
        self.settings = settings
        self.stagers = []
        self.started = False
        self.received_url_quota = time.time()
        self._filenames_set = filenames_set
        self._finished_urls_set = finished_urls_set
        self._found_urls_set = found_urls_set
        self._job = Job(self.identifier, filenames_set, finished_urls_set,
                        found_urls_set)
        self._urls = {}
        self._url_database = UrlDeduplicationDatabase(self.identifier,
            'crawler_' + self.identifier)
        logger.debug('Created crawler job %s.', self)

    def add_stager(self, s):
        """Adds a stager server to the project.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server to add
                to the job.
        """
        logger.debug('Adding stager %s to crawler job %s.', s, self)
        if s in self.stagers:
            logger.warning('Stager %s already added to crawler job %s.', s,
                           self)
            return None
        self.stagers.append(s)

    def add_url(self, s, urlconfig):
        """Adds an URL to the job.

        If the URL is not archived yet, it is added to the job and to the list
        of URLs to be crawled.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the URL to be added.
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration
                of the URL to be checked.
        """
        logger.debug('Adding URL %s from stager %s to crawler job %s.',
                     urlconfig, s, self)
        if self.archived_url(urlconfig):
            logger.debug('URL %s is already archived.', urlconfig)
            return None
        self._urls[urlconfig] = s
        self._job.add_url(urlconfig)

    def increase_url_quota(self, i):
        """Increases the URL quota.

        Args:
            i (int): The number to increase the URL quota with.
        """
        logger.debug('Crawler job %s received %s URLs quota.', self, i)
        self._received_url_quota = time.time()
        self._job.increase_url_quota(i)

    def get_url_stager(self, urlconfig):
        """Gets an URL from the URLs dict.

        Args:
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration
                of the URL to be checked.
        """
        return self._urls[urlconfig]

    def delete_url_stager(self, urlconfig):
        """Deletes an URL from the URLs dict.

        Args:
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration
                of the URL to be checked.

        Returns:
            bool: False if the URL is not known to the job.
        """
        logger.debug('Deleting URL %s from crawler job %s.', urlconfig, self)
        if urlconfig not in self._urls:
            logger.warning('URL %s not in crawler job%s.', urlconfig)
            return False
        del self._urls[urlconfig]
        return True

    def start(self):
        """Starts the job.

        If the job is not yet started, it will be started. The crawl will
        begin.

        Returns:
            bool: True is the crawl is started succesfully.
            NoneType: If the crawl is already started.
        """
        logger.debug('Starting crawler job %s.', self)
        if self.is_started:
            logger.warning('Crawler job %s already started.', self)
            return None
        self._job.start()
        self.started = True
        return True

    def finished_url(self, urlconfig):
        """Adds a finished URL to the database.

        Args:
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration
                of the URL to be checked.
        """
        logger.debug('URL %s finished for crawler job %s.', urlconfig, self)
        self._url_database.insert(urlconfig)

    def archived_url(self, urlconfig):
        """Checks if an URL is already archived.

        Args:
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration
                of the URL to be checked.

        Returns:
            bool: True if the URL is already archived, else False.
        """
        return self._url_database.has_url(urlconfig.url)

    def allowed_url(self, urlconfig):
        """Checks if an URL is allowed to be crawled.

        This check is done by checking if the URLs matched one or more of the
        allowed regular expressions and none of the ignored regular
        expressions, it is also checked if the depth of the URL is not larger
        than the maximum allowed depth.

        Args:
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration
                of the URL to be checked.

        Returns:
            bool: True if the URL is allowed, False if not.
        """
        for regex in self.settings.allow_regex:
            if re.search(regex, urlconfig.url):
                break
        else:
            return False
        for regex in self.settings.ignore_regex:
            if re.search(regex, urlconfig.url):
                return False
        if urlconfig.depth > self.max_depth:
            return False
        return True

    @property
    def max_depth(self):
        """int: The maximum depth the job is allowed to go."""
        return self.settings.depth

    @property
    def is_started(self):
        """bool: Whether the job has started."""
        return self.started or self._job.ident

    @property
    def identifier(self):
        """str: The job identifier."""
        return self.settings.identifier

    @property
    def finished(self):
        """bool: True if crawler is not active."""
        return len(self._urls) == 0 and len(self._filenames_set) == 0 \
            and len(self._finished_urls_set) == 0 \
            and len(self._found_urls_set) == 0

    def __repr__(self):
        return '<{} at 0x{:x} job={}>' \
            .format(__name__, id(self), self.settings.identifier)

