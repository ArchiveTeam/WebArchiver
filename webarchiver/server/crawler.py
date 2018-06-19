"""The crawler server."""
import functools
import logging
import os
import socket
import time

from webarchiver.config import *
from webarchiver.database import UrlDeduplicationDatabase
from webarchiver.server.base import BaseServer, Node
from webarchiver.server.job import CrawlerServerJob
from webarchiver.server.node import CrawlerNode
from webarchiver.set import LockedSet
from webarchiver.utils import check_time, key_lowest_value, sample

logger = logging.getLogger(__name__)


class CrawlerServer(BaseServer):
    """The crawler server and attributes for the commands."""

    def __init__(self, stager_host, stager_port, host=None, port=None):
        """Inits the crawler server.

        The crawler server connects to a stager server and announces itself.

        Args:
            host (str, optional): The host to use for the listener.
            port (int, optional): The port to use for the listener.
            stager_host (str): The host of the stager to connect to.
            stager_port (int): The port of the stager to connect to.
        """
        super().__init__(host, port)
        self._stager = {}
        self.add_stager((stager_host, stager_port))
        self._jobs = {}
        self._upload_permissions = UploadPermissions()
        self._filenames_set = LockedSet()
        self._finished_urls_set = LockedSet()
        self._found_urls_set = LockedSet()
        self._last_upload_request = 0
        self._last_url_quota = 0
        logger.info('Created crawler server.')

    def _run_round(self):
        """Runs the base server loop and functions specific for this server.

        The :func:`webarchiver.server.base.BaseServer._run_round` function is
        run, together with a function to request new stager servers, ping
        upload WARC files, process finished and discovered URLs and request URL
        quotas from a stager server.
        """
        super()._run_round()
        self.request_stager()
        self.ping()
        self.upload()
        self.finish_urls()
        self.found_urls()
        self.request_url_quota()

    def _create_socket(self, address):
        """Creates and connects a :class:`webarchiver.server.base.Node` and
        adds this to the write queue.

        Args:
            address (tuple): A tuple (host, port) to connect to.
        """
        logger.debug('Creating connection with listener %s.', address)
        s = self._connect_socket(address)
        self._write_queue[s] = []
        return s

    def add_stager(self, listener, extra=False, s=None):
        """Adds a stager server to the network.

        Creates or uses a connected :class:`webarchiver.server.base.Node` of a
        stager that the crawler server is not yet connected to. The new stager
        is added to the read list and the message is send to the stager
        server::

            ANNOUNCE_CRAWLER <own listener>

        In case this is not the first stager server ``ANNOUNCE_CRAWLER_EXTRA``
        is used instead of ``ANNOUNCE_CRAWLER``.

        Args:
            listener (tuple): A tuple (host, port) to use for the new stager
                server.
            extra (bool, optional): Whether the stager server is not the first.
                Default value is False.
            s (:obj:`webarchiver.server.base.Node`, optional):
                :class:`webarchiver.server.base.Node` for the new stager
                server, if specified it will be used instead of making a new
                connection.

        Returns:
            bool: True.
        """ #TODO returns
        logger.debug('Adding stager %s.', listener)
        for s_ in self._stager:
            if s_.listener == listener:
                logger.warning('Stager %s already added.', listener)
                return None
        s_ = self._create_socket(listener) if not s else s
        self._read_list.append(s_)
        self._stager[s_] = CrawlerNode()
        self._write_socket_message(s_, 'ANNOUNCE_CRAWLER' \
                                   + ('_EXTRA' if extra else ''),
                                   self._address)
        return True

    def request_stager(self):
        """Requests new stager server to connect to.

        The new stager server are needed and the last request was long enough
        ago a request is made to a random stager server to send a certain
        number of stager server the crawler server is not connected to::

            REQUEST_STAGER <number stagers needed> <listeners of connected
                stager servers>
        """
        if self.stager_needed > 0 and check_time(self._last_stager_request,
                                                  REQUEST_STAGER_TIME):
            self._write_socket_message(sample(self._stager, 1),
                                       'REQUEST_STAGER', self.stager_needed,
                                       *[s.listener for s in self._stager])
            self._last_stager_request = time.time()

    def ping(self):
        """Pings all stager servers.

        Every ``PING_TIME`` second a ping message is send to each stager server::

            PING

        When the message is send the servers are set not having replied with a
        pong.
        """
        if check_time(self._last_ping, PING_TIME):
            for s in self._stager:
                self._stager[s].pong = False
            self._write_socket_message(self._stager, 'PING')
            self._last_ping = time.time()

  #  def request_upload(self):
  #      if check_time(self._last_upload_request, config.REQUEST_UPLOAD_TIME):
  #          self._write_socket_message(self._jobs[job]['stager'],
  #                                     'REQUEST_UPLOAD')
  #          self._last_upload_request = time.time()
  #          time.sleep(1)

    def upload(self):
        """Uploads WARC files that ready to be uploaded.

        A request for upload permission is send to each stager server connected
        to a job for each WARC file ready to be uploaded::

            REQUEST_UPLOAD_PERMISSION <job identifier> <WARC path>
                <WARC filesize>

        The WARC file will be send to a random stager server that allowed
        upload. The other stager servers will be send a message to revoke the
        upload permission::

            REQUEST_UPLOAD_REVOKE <job identifier> <WARC path>

        If no server responded to the request in time, the request is reset.
        Both requested and received data is saved in a :class:`WarcFile`
        object.
        """
        if len(self._filenames_set) == 0:
            return None
        logger.debug('Uploading WARC files.')
        with self._filenames_set.lock:
            for job, path in self._filenames_set:
                warc_file = self._upload_permissions[path]
                if not warc_file.requested:
                    self._write_socket_message(self._jobs[job].stager,
                                               'REQUEST_UPLOAD_PERMISSION',
                                               job, path, warc_file.filesize)
                    warc_file.requested = True
                    continue
                if warc_file.chosen is False:
                    logger.debug('Resetting requests for WARC file %s for not'
                                  ' chosing server in time.', warc_file)
                    del self._upload_permissions[path]
                elif warc_file.chosen is not None and not warc_file.revoked:
                    self._write_socket_message(warc_file.to_revoke,
                                               'REQUEST_UPLOAD_REVOKE', job,
                                               path)
                    warc_file.revoked = True
                    self.upload_warc(warc_file.chosen, job, path)

    def upload_warc(self, s, job, path):
        """Uploads a WARC file.

        A temporary file for the path with extension ``.uploading`` is created
        to show the file is being uploaded. If this ``.uploading`` file
        already exists, the file is not uploaded. The file is uploaded using
        the :func:`_write_socket_file` function::

            WARC_FILE <path> <file> <job identifier>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The
                :class:`webarchiver.server.base.Node` to send the file to.
            job (str): The job identifier.
            path (str): The path to the file.
        """
        if os.path.isfile(path + '.uploading'):
            return False
        open(path + '.uploading', 'w').close()
        self._write_socket_file(s, path, 'WARC_FILE', job)
        return True #TODO confirmation?

    def finish_urls(self):
        """Confirms to the stager server which URLs finished.

        After a crawl finishes the finished URLs are added to a set of
        finished URLs. Each finished URL is send to all stager server connected
        to the job::

            JOB_URL_FINISHED <job_identifier> <URL> <listener that queued URL>

        The URLs are in the finished URLs set as
        :class:`webarchiver.url.UrlConfig` objects. After being send an URL is
        deleted from the job and from the set of finished URLs.
        """
        if len(self._finished_urls_set) == 0:
            return None
        finished = set()
        logger.debug('Reporting finished URLs.')
        with self._finished_urls_set.lock:
            for urlconfig in self._finished_urls_set:
                print(self._jobs)
                identifier = urlconfig.job_identifier
                job = self._jobs[identifier]
                job.finished_url(urlconfig)
                self._write_socket_message(job.stager, 'JOB_URL_FINISHED',
                                           urlconfig.job_identifier,
                                           urlconfig.url,
                                           job.get_url_stager(urlconfig) \
                                               .listener)
                finished.add(urlconfig)
                job.delete_url_stager(urlconfig)
                print(self._jobs)
            self._finished_urls_set.difference_update(finished)

    def found_urls(self):
        """Reports discovered URLs to the stager servers.

        The URLs discovered in a crawl are added to a found URLs set. They
        are in :class:`webarchiver.url.UrlConfig` objects. Each URL is send to
        a randomly chosen stager server connected to the job the URL was
        discovered in::

            JOB_URL_DISCOVERED :obj:`webarchiver.url.UrlConfig`

        Send URLs are removed from the set.
        """
        if len(self._found_urls_set) == 0:
            return None
        finished = set()
        logger.debug('Reporting finished URLs.')
        with self._found_urls_set.lock:
            for urlconfig in self._found_urls_set:
                finished.add(urlconfig)
                identifier = urlconfig.job_identifier
                if self._jobs[identifier].archived_url(urlconfig):
                    continue
                print(urlconfig.url, urlconfig.parent_url, urlconfig.depth)
                if not self._jobs[identifier].allowed_url(urlconfig):
                    continue
                print('passed', urlconfig.url, urlconfig.parent_url,
                      urlconfig.depth)
                stager = sample(self._jobs[identifier].stager, 1)[0]
                self._write_socket_message(stager, 'JOB_URL_DISCOVERED',
                                           urlconfig)
            self._found_urls_set.difference_update(finished)

    def request_url_quota(self):
        """Requests a quotum for URLs to crawl for a job.

        After `URL_QUOTA_TIME` seconds a random stager connected to a running
        job is asked for an URL quota for URLs to be crawled::

            REQUEST_URL_QUOTA <job identifier>
        """
        if check_time(self._last_url_quota, URL_QUOTA_TIME):
            self._last_url_quota = time.time()
            if len(self._jobs) == 0:
                return None
            job = key_lowest_value({
                job: self._jobs[job].received_url_quota
                for job in self._jobs
            })
            self._write_socket_message(sample(self._jobs[job].stager, 1)[0],
                                       'REQUEST_URL_QUOTA', job)

    def create_job(self, settings):
        """Creates a new job.

        If the identifier from the given settings for a job is not yet used by
        a job a new job will be created for it.

        A job is created as :class:`webarchiver.server.job.CrawlerServerJob`
        object using the given job settings.

        Note:
            Each stager server will send a command to the each crawler server
            to create a certain job, so it is made sure a job is only added
            once.

        Args:
            settings (:obj:`webarchiver.job.settings.JobSettings`): The
                settings for the new job.
        """
        logger.debug('Creating job %s.', settings)
        if settings.identifier in self._jobs:
            logger.warning('Job %s already created.', settings)
            return None
        self._jobs[settings.identifier] = \
            CrawlerServerJob(settings, self._filenames_set,
                             self._finished_urls_set, self._found_urls_set)

    def start_job(self, identifier):
        """Starts a job.

        Args:
            identifier (str): The job identifier to be started.

        Returns:
            bool: False if the job identifier is not known, True if the project
                is succesfully started.
        """ #TODO True right?
        logger.debug('Starting job %s.', identifier)
        if identifier not in self._jobs:
            logger.warning('Job %s does not exist.', identifier)
            return False
        return self._jobs[identifier].start()

    def job_add_stager(self, identifier, s):
        """Adds a stager server to a job.

        A stager server that announces it is (one of) the stager servers that
        runs a certain job is added to the job.

        Args:
            identifier (str): The job identifier.
            s (:obj:`webarchiver.server.base.Node`): The
                :class:`webarchiver.server.base.Node` that is added.

        Returns:
            bool: False if the job identifier is not known, True if the  stager
                server is succesfully added.
        """
        logger.debug('Adding stager %s to job %s.', s, identifier)
        if identifier not in self._jobs:
            logger.warning('Job %s does not exist.', identifier)
            return False
        self._jobs[identifier].add_stager(s)
        return True

    def job_add_url(self, s, urlconfig):
        """Adds an URL to a job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The
                :class:`webarchiver.server.base.Node` that queued the URL to be
                added.
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration and
                settings of the URL to be added.

        Returns:
            bool: False if the job identifier is not known, True if the URL is
                succesfully added.
        """
        logger.debug('Adding URL %s.', urlconfig)
        if urlconfig.job_identifier not in self._jobs:
            logger.debug('Job of URL %s not found.', urlconfig)
            return False
        self._jobs[urlconfig.job_identifier].add_url(s, urlconfig)
        return True

    def _command_pong(self, s, message):
        """Processes the ``PONG`` command.

        Send a ping if this is a faulty pong message, else sets the stager
        stager server to having send a pong.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    PONG
        """
        if self._stager[s].pong is False:
            self._stager[s].pong = True
        else:
            logger.info('Pong was send from %s without initial ping.', s)
            self.ping()

    def _command_confirmed(self, s, message):
        """Processes the ``CONFIRMED`` command.

        Set the server to having confirmed and confirm back if needed::

            CONFIRMED 1

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    CONFIRMED <confirmed state>
        """
        self._stager[s].confirmed = True
        if message[1] == 0:
            self._write_socket_message(s, 'CONFIRMED', 1)

    def _command_assigned_url_quota(self, s, message):
        """Processes the ``ASSIGNED_URL_QUOTA`` command.

        The quota of the job for this crawler server is updated with the new
        assigned quota.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    ASSIGNED_URL_QUOTA <job identifier> <assigned quota>
        """
        self._jobs[message[1]].increase_url_quota(message[2])

    def _command_new_job_crawl(self, s, message):
        """Processes the ``NEW_JOB_CRAWL`` command.

        Creates a new job using the given job settings. A message is send back
        to confirm the job is added::

            JOB_CRAWL_CONFIRMED <job identifier>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    NEW_JOB_CRAWL :obj:`webarchiver.job.settings.JobSettings`
        """
        self.create_job(message[1])
        self.job_add_stager(message[1].identifier, s)
        self._write_socket_message(s, 'JOB_CRAWL_CONFIRMED',
                                   message[1].identifier)

    def _command_job_url_crawl(self, s, message):
        """Processes the ``JOB_URL_CRAWL`` command.

        Adds a new :class:`webarchiver.url.UrlConfig` object to a job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    JOB_URL_CRAWL :obj:`webarchiver.url.UrlConfig`
        """
        self.job_add_url(s, message[1])

    def _command_job_start_crawl(self, s, message):
        """Processes the ``JOB_START_CRAWL`` command.

        Starts a job. A message is send back to confirm the crawl has started::

            JOB_STARTED_CRAWL <job identifier>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    JOB_START_CRAWL <job identifier>
        """
        self.start_job(message[1])
        self._write_socket_message(s, 'JOB_STARTED_CRAWL', message[1])

#    def _command_upload_requested(self, s, message):
#        self._jobs[s]['upload'] = eval(message[2]) #FIXME

    def _command_warc_file_received(self, s, message):
        """Processes the ``WARC_FILE_RECEIVED`` command.

        The stager server messages that the WARC file is received. The files
        for the WARC file and the WARC file itself are removed.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    WARC_FILE_RECEIVED <job identifier> <path to WARC file>
        """
        logger.debug('Removing WARC file %s.', message[2])
        self._filenames_set.remove((message[1], message[2]))
        del self._upload_permissions[message[2]]
        os.remove(message[2])
        os.remove(message[2] + '.uploading')

    def _command_add_stager(self, s, message):
        """Processes the ``ADD_STAGER`` command.

        Adds a stager to the crawler server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    ADD_STAGER <listener of the stager server>
        """
        r = self.add_stager(message[1], extra=True)
#        if r is None: TODO don't have to report this(?)
#            self._write_socket_message(s, 'STAGER_ALREADY_ADDED', *listener)
#        elif r is true:
#            self._write_socket_message(s, 'STAGER_ADDED', *listener)

    def _command_upload_permission_granted(self, s, message):
        """Processes the ``UPLOAD_PERMISSION_GRANTED`` command.

        The upload of a WARC file is permitted. Sets the upload permission for
        the stager server for the WARC file.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    UPLOAD_PERMISSION_GRANTED <job identifier> <WARC path>
        """
        warc_file = self._upload_permissions[message[2]]
        if not warc_file.requested:
            return False
        warc_file.granted(s)

    def _command_upload_permission_denied(self, s, message):
        """Processes the ``UPLOAD_PERMISSION_DENIED`` command.

        The upload permission was denied. Currently no functionality.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    UPLOAD_PERMISSION_DENIED <job identifier> <WARC path>
        """
        pass

    def _command_already_confirmed(self, s, message):
        """Processes the ``ALREADY_CONFIRMED`` command.

        The crawler server is already connected to by the stager server.
        Currently not functionality.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    ALREADY_CONFIRMED
        """
        pass # TODO

    @property
    def stager_needed(self):
        """Checks how many stager servers can connect.

        Returns:
            int: The number of stager servers that can connect. This is not
                lower than 0.
        """
        n = MAX_STAGER - len(self._stager)
        return 0 if n < 0 else n

    def __repr__(self):
        return '<{} at 0x{:x} listener={}>'.format(__name__, id(self),
                                                   self._address)


class UploadPermissions(dict):
    """The class to hold upload permissions for files.

    The requests for permissions to upload are send by crawler server to stager
    servers when an URL is ready to be uploaded.

    This class is a dict, it has all the same functions. Only if an item does
    not exist in the the dict, it is created.

    Note:
        It is not needed to add items to the object, since these are added
        automatically if missing.
    """

    def __init__(self):
        super().__init__(self)

    def __getitem__(self, path):
        """Gets an item from the dict.

        If the item does not exist, it is created new. The newly created file
        is a :class:`WarcFile` object.

        Args:
            path (str): Path to the file.

        Returns:
            :obj:`WarcFile`: An already created or newly created
                :class:`WarcFile` object for the requested file.
        """
        if path not in self:
            super().__setitem__(path, WarcFile(path))
        return super().__getitem__(path)


class WarcFile:
    """The class to hold a file and keep track of upload permissions.

    Attributes:
        requested (bool): Whether the permission has been requested for the
            file. Default value is False.
        revoked (bool): Whether a request to revoke permissions has been done
            for the file. Default value it False.
    """

    def __init__(self, path):
        """Inits the :class:`WarcFile` object.

        Args:
            path (str): The path to the file the :class:`WarcFile` object is
                created for.
        """
        self.requested = False
        self.revoked = False
        self._granted = []
        self._last_answer = 0
        self._path = path

    def granted(self, s):
        """Sets the permission of a stager server to granted.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                gave permission for the upload of the file.
        """
        self._granted.append(s)
        self._last_answer = time.time()

    @property
    def to_revoke(self):
        """Returns objects to ask to revoke permissions from.

        If a stager server was selected for the file upload it will not be
        included in the list to be returned.

        Returns:
            list: The list contains :class:`webarchiver.server.base.Node`
                objects that can have their permission revoked.
        """
        return [s for s in self._granted if s != self.chosen]

    @property
    @functools.lru_cache()
    def filesize(self):
        """Gets the file size of the file.

        Note:
            Once calculate, the same calculate file size will be returned on
                every call.

        Returns:
            int: File size of the file.
        """
        return os.path.getsize(self._path)

    @property
    def chosen(self):
        """Chooses a stager server for upload.

        A stager server is only selected if there is at least one permission
        for upload. A chosen server is remembered.

        Note:
            Behavior by other implemented functions using this attribute is to
            delete the WarcFile is False is received, since there is no
            implementation to 'reset' a :class:`WarcFile` object.

        Returns:
            :obj:`webarchiver.server.base.Node` or bool or NoneType: None if
                ``REQUEST_UPLOAD_TIME`` seconds have not passed yet or if no
                previous request was made and no permissions have been received
                yet. If permission have been received and
                ``REQUEST_UPLOAD_TIME`` seconds have passes, a
                :class:`webarchiver.server.base.Node` object for chosen stager
                server is returned.

                If a decision was previously made, the same decision is
                returned.
        """
        if not hasattr(self, '_chosen'):
            if self._last_answer == 0:
                self._last_answer = time.time()
                return None
            if not check_time(self._last_answer, REQUEST_UPLOAD_TIME):
                return None
            if len(self._granted) == 0:
                return False
            self._chosen = sample(self._granted, 1)
        return self._chosen

    def __repr__(self):
        return '<{} at 0x{:x} WARC file={}>'.format(__name__, id(self),
                                                    self._path)

