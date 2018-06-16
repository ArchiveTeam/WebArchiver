"""Configuration for a job on a stager server."""
import time

from webarchiver.config import *
from webarchiver.server.base import Node
from webarchiver.url import init_urls
from webarchiver.utils import sample, split_set


class StagerServerJob:
    """This holds the configuration for a job on a stager server.

    The stager server communicates with this to set and use configurations of
    the assigned job, like other servers working on this job and backups of
    URLs.

    Attributes:
        settings (:obj:`webarchiver.job.settings.JobSettings`): The settings
            for the job.
        initial (bool): Whether this is the initial stager server for this job.
        initial_stager (:obj:`webarchiver.server.base.Node`): The initial
            server that created the job. This is the server that loaded the job
            first and spread it among other stager servers.
        discovered_urls (dict): A dict like::

                {<URL>: :job:`webarchiver.url.UrlConfig`}

            The dict contains the URL as key and the configuration of the URL
            as value.
        current_urls (dict): A dict like ``discovered_urls`` with URLs
            currently assigned to the tracker.
        finished (bool): True if the job is finished, else False.
        crawlers (dict): A dict with items like::

                {:obj:`webarchiver.server.base.Node`:
                :obj:`StagerServerJobCrawler`}

            This contains all crawler servers connected to the stager server
            that are assigned to the job.
        stagers (dict): A dict with items like::

                {:obj:`webarchiver.server.base.Node`:
                :obj:`StagerServerJobStager`}

            This contains all stager servers that are assigned to the job.
        backup (dict): A dict with items like::

                {<listener>: {<URL>: :job:`webarchiver.url.UrlConfig`}}

            This contains for each listeners of each stager server in
            ``stagers`` a backup of some of the URLs assigned to this stager
            server. Each URL is backed up in a dict with the URL as key and
            URL configuration as value.
    """

    def __init__(self, settings, initial, initial_stager=None):
        """Inits the job configuration.

        Args:
            settings (:obj:`webarchiver.job.settings.JobSettings`): The
                settings for the job.
            initial (bool): Whether this is the initial stager server for this
                job. If it is, ``initial_stager`` should be left as None.
            initial_stager (:obj:`webarchiver.server.base.Node`): The initial
                server that created the job. This is the server that loaded the
                job first and spread it among other stager servers.
        """
        self.settings = settings
        self.initial = initial
        self.initial_stager = initial_stager
        self.discovered_urls = init_urls(self.identifier, self.initial_urls) \
            if self.initial else {}
        self.current_urls = {}
        self.finished = False

        self.crawlers = {}
        self.stagers = {}
        self.backup = {}

    def add_crawler(self, s):
        """Adds a crawler server to the job.

        The crawler server is added to the ``crawlers`` dict with the server as
        key and a :class:`StagerServerJobCrawler` object as value to hold the
        configuration of this specific connected crawler server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server to add.
        """
        if s in self.crawlers:
            return None
        self.crawlers[s] = StagerServerJobCrawler(s)

    def crawler_confirmed(self, s):
        """Confirmes the crawler server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server to
                confirm.
        """
        self.crawlers[s].confirmed = True

    def add_stager(self, s):
        """Adds a stager server to the job.

        The stager server is added to the ``stagers`` dict with the server as
        key and a :class:`StagerServerJobStager` object as value to hold the
        configuration of this specific connected stager server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server to add.
        """
        self.stagers[s] = StagerServerJobStager(s)
        self.backup[s.listener] = {}

    def backup_url(self, s, urlconfig):
        """Backs an URL up.

        Creates a backup of an URL for a specific stager server. The URL is
        backed up into the ``backup`` dict with the listener of the stager
        server as key and a dict of the URL and URL configuration as backup.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server to
                backup the URL for.
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The URL configuration
                to backup.
        """
        self.backup[s.listener][urlconfig.url] = urlconfig

    def share_urls(self):
        """Shares the discovered URLs over the stager servers.

        The set of discovered URLs is split into smaller sets according to the
        number of stager servers that are on the job. A set is assigned to this
        stager server. The other sets are assigned to other stager servers. For
        each set of URLs a number of ``MAX_BACKUPS`` stager servers are chosen
        for backup of the URL.

        Yields:
            tuple: Each URL in a set for a certain stager server is yielded for
                sending to this stager server like::

                    (<URL configuration>, :obj:`webarchiver.server.base.Node`,
                    list of :obj:`webarchiver.server.base.Node`)

                Which is a tuple of the URL configuration, the stager server
                the URL is assigned to and a list of backups.

                For URLs assigned to this current stager server,
                :obj:`webarchiver.server.base.Node` is None.
        """
        if len(self.discovered_urls) == 0:
            return None
        url_lists = split_set(self.discovered_urls, len(self.stagers)+1)
        backups = sample(self.stagers, MAX_BACKUPS)
        for urlconfig in url_lists.pop():
            #self.add_url_crawler(url)
            yield urlconfig, None, backups
            del self.discovered_urls[urlconfig.url]
        for s in self.stagers:
            backups = sample(['this'] + [s_ for s_ in self.stagers if s_ != s],
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
        """Adds a crawler to an URL.

        The URL is added to the list of current URLs and added to the
        configuration of the crawler server.

        Args:
            :obj:`webarchiver.url.UrlConfig`: The URL to assign.
        """
        crawler = sample(self.crawlers, 1)[0]
        self.current_urls[urlconfig.url] = urlconfig
        self.crawlers[crawler].add_url(urlconfig.url)
        return crawler

    def add_url(self, urlconfig):
        """Adds an URL to the discovered URLs.

        Args:
            :obj:`webarchiver.url.UrlConfig`: The discovered URL to add.
        """
        self.discovered_urls[urlconfig.url] = urlconfig

    def finish_url(self, s, url, listener):
        """Finishes an URL.

        If the stager server that finished the URL is as backup the URL is
        removed from backup, else it is removed from the current URLs and the
        configuration of the crawler server it was assigned to.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server which
                finished the URL.
            url (str): The finished URL.
            listener (tuple): The listener of the stager server that finished
                the URL.
        """
        if listener in self.backup:
            #job['urls'].remove(url) #TODO should this be currently crawling or currently using
            del self.backup[listener][url]
        else:
            print(self.current_urls)
            del self.current_urls[url]
            self.crawlers[s].remove_url(url)

    def confirmed(self, s, i):
        """Confirmes a stager server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server to
                confirm.
            i (int): The state of the confirmation.
        """
        if  self.stagers[s].confirmed:
            return None
        self.stagers[s].confirmed = True
        if i == 0:
            return 1
        elif i == -1:
            return -1 # TODO  waiting for each s to add job?

    def started_stager(self, s):
        """Sets a stager server to having started the job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                started the job.
        """
        self.stagers[s].started = True

    def started_crawl(self, s):
        """Sets a crawler server to having started the job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                started the job.
        """
        self.crawlers[s].started = True

    def add_counter(self, s):
        """Sets a stager server as counter for the URL quota for the job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server to set
                as counter.
        """
        self.counter = s

    def set_as_counter(self):
        """Sets this stager server as counter.

        The time in seconds is used as counter to keep track of the number of
        URLs that can be allowed to be crawled since the last URL quota
        request.
        """
        self.counter = time.time()

    @property
    def rate(self):
        """int: The rate in URLs per second of the job."""
        return self.settings.rate

    @property
    def identifier(self):
        """str: The job identifier."""
        return self.settings.identifier

    @property
    def initial_urls(self):
        """tuple of str: The set of the initial URLs of the job."""
        if self.initial:
            return self.settings.urls
        return ()

    @property
    def started(self):
        """bool: True if all stager servers started the crawl, else False."""
        if not self.started_local_crawl:
            return False
        for c in self.stagers.values():
            if not c.started:
                return False
        return True

    @property
    def started_local_crawl(self):
        """bool: True if all crawler servers started the crawl, else False."""
        for c in self.crawlers.values():
            if not c.started:
                return False
        return True

    @property
    def stager_confirmed(self):
        """bool: True if all stager servers confirmed the job, else False."""
        for c in self.stagers.values():
            if not c.confirmed:
                return False
        return True

    @property
    def url_quota(self):
        """int: The URL quota for the job."""
        assert self.is_counter
        old = self.counter
        self.counter = time.time()
        return int((self.counter-old)*self.rate)

    @property
    def is_counter(self):
        """bool: True if the current stager server is the URL quota counter,
        else False.
        """
        return type(self.counter) == float

    @property
    def counter(self):
        """:obj:`webarchiver.server.base.Node` or float: The URL quota counter
        of this job.
        """
        if not hasattr(self, '_counter'):
            return None
        return self._counter

    @counter.setter
    def counter(self, value):
        assert type(value) in {float, Node}
        self._counter = value


class StagerServerJobCrawler:
    """The configuration for a crawler server assigned to the job.

    Attributes:
        s (:obj:`webarchiver.server.base.Node`): The crawler server.
        confirmed (bool): Whether the crawler server is confirmed.
        started (bool): Whether the crawler server started the job.
        urls (set of str): The URLs assigned to the crawler server.
    """

    def __init__(self, s):
        """Inits the crawler server configuration.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server.
        """
        self.s = s
        self.confirmed = False
        self.started = False
        self.urls = set()

    def add_url(self, url):
        """Adds an URL to the crawler server.

        Args:
            url (str): The URL to add.
        """
        self.urls.add(url)

    def remove_url(self, url):
        """Removes an URL from the crawler server.

        Args:
            url (str): The URL to removed.
        """
        self.urls.remove(url)


class StagerServerJobStager:
    """The configuration for a stager server assigned to the job.

    Attributes:
        s (:obj:`webarchiver.server.base.Node`): The stager server.
        confirmed (bool): Whether the stager server is confirmed.
        started (bool): Whether the stager server started the job.
    """

    def __init__(self, s):
        """Inits the stager server configuration.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server.
        """
        self.s = s
        self.confirmed = False
        self.started = False

__all__ = ('StagerServerJob',)

