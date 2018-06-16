"""The stager server."""
import os
import socket
import threading
import time

from webarchiver.config import *
from webarchiver.job import new_jobs
from webarchiver.server.job import StagerServerJob
from webarchiver.server.base import BaseServer, Node
from webarchiver.server.node import StagerNodeCrawler, StagerNodeStager
from webarchiver.utils import check_time, sample, write_file


class StagerServer(BaseServer):
    """The stager server and attributes for the commands."""

    def __init__(self, stager_host=None, stager_port=None, host=None,
                 port=None):
        """Inits the stager server.

        The stager server can be started on its own or can be given an host and
        port of another stager to connect to. If a connection is made with
        another stager server, this stager server is added to the other already
        existing network and will start working on assigned jobs.

        Args:
            host (str, optional): The host to use for the listener.
            port (int, optional): The port to use for the listener.
            stager_host (str, optional): The host of the stager to connect to.
            stager_port (int, optional): The port of the stager to connect to.
        """
        super().__init__(host, port)
        self._data_sockets = []
        self._urls = None#Urls()
        self._stager = {}
        self._listeners = {}
        self._crawlers = {}
        if stager_host is not None and stager_port is not None:
            self.init_stager((stager_host, stager_port))
        self._jobs = {}
        self._last_jobs_check = 0
        self._used_space = 0
        self._uploading = {}
        self.test = 0 #TODO TEMP

        self._job_checker = threading.Thread(target=self._get_jobs)
        self._job_checker.daemon = True
        self._job_checker.start()

    def _run_round(self):
        """Runs the base server loop and functions specific for this server.

        The :func:`webarchiver.server.base.BaseServer._run_round` function is
        run, after which a few server specific function are run. The server
        will ping, start jobs and share the newly discovered URLs of jobs.
        """
        super()._run_round()
        self.ping()
        if os.path.isfile('starttest' + str(self._address[1])) and len(self._jobs) == 0:
            with open('starttest' + str(self._address[1]), 'r') as f:
                self.create_job('testjob', [s.strip() for s in f.read().splitlines()])
        for job in self._jobs:
            #for s in self._jobs[job_]['stager']:
            #    if self._jobs[job_]['stager'][s]['started' TODO
            #print(self._jobs[job].initial_stager == self._socket, self._jobs[job].stager_confirmed, self.test == 0, not self._jobs[job].started)
            if self._jobs[job].initial_stager == self._socket \
                    and self._jobs[job].stager_confirmed and self.test == 0 \
                    and not self._jobs[job].started:
                self.start_job(job)
                self.test = 1
        self.check_jobs()

    def _get_jobs(self):
        """Adds new jobs.

        Each newly discovered job is created and added.
        """
        for job in new_jobs():
            self.create_job(job)

    def _read_socket(self, s):
        """Accept and adds incoming connections and reads from servers.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The node of the connected
                server the incoming message is from.
        """
        if s == self._socket:
            server, address = self._socket.accept()
            server = Node(server)
            self._read_list.append(server)
            self._write_queue[server] = []
        else:
            super()._read_socket(s)

    def init_stager(self, listener, extra=False):
        """Inits the connection to a stager server.

        Using the listener a connection is created to a stager server. The
        stager server is added to the read list and write queue and is added to
        this stager server. When connected a message is send to announce
        itself with its own listener::

            ANNOUNCE_STAGER <listener>

        Or in case the stager server is not the initial stager server
        ``ANNOUNCE_STAGER_EXTRA`` is used instead of ``ANNOUNCE_STAGER``.

        Note:
            This function is used for both the initial stager server to connect
            to and later new stager servers.

        Args:
            listener (tuple): A tuple (host, port) of the stager server to
                connect to.
            extra (bool): Whether this is the initial stager server to connect
                to or not.

        Returns:
            bool: True if the the stager server is added.
            NoneType: If the stager server is already added.
        """
        if listener in self._listeners:
            return None
        s = self._connect_socket(listener)
        self._write_queue[s] = []
        self._read_list.append(s)
        #print(s)
        self.add_stager(s, listener)
        self._write_socket_message(s, 'ANNOUNCE_STAGER' \
                                   + ('_EXTRA' if extra else ''),
                                   self._address)
        return True

    def ping(self):
        """Pings the connected servers.

        The servers ping both the crawler and stager servers::

            PING

        The pong variable for each server it set to False, so a pong is
        expected from the server.
        """
        if check_time(self._last_ping, PING_TIME):
            for s in self._stager:
                self._stager[s].pong = False
            self._write_socket_message(self._stager, 'PING')
            for s in self._crawlers:
                self._crawlers[s].pong = False
            self._write_socket_message(self._crawlers, 'PING')
            self._last_ping = time.time()

    def check_jobs(self):
        """Share the URLs of each job with other staging servers.

        The initial or discovered URLs for each job need to be spread over
        other stager servers to be archived. The ``share_urls`` function of a
        job yields a list of URLs, assigned stager servers and backup location.
        An URL is send to its assigned stager server::

            JOB_URL :obj:`webarchiver.url.UrlConfig`

        and to the backup location the URL is send::

            JOB_URL_BACKUP :obj:`webarchiver.url.UrlConfig`
                <listener assigned server>

        Note:
            If the server value of the yielded data is None, the current stager
            server has the URL assigned.
        """
        if check_time(self._last_jobs_check, JOBS_CHECK_TIME):
            for job in self._jobs.values():
                for urlconfig, s, backups in job.share_urls():
                    if s is None:
                        s = self._socket
                        self._command_job_url(None, [None, urlconfig])
                    else:
                        self._write_socket_message(s, 'JOB_URL', urlconfig)
                    self._write_socket_message(backups, 'JOB_URL_BACKUP',
                                               urlconfig, s.listener)
            self._last_jobs_check = time.time()

    def create_job(self, settings, initial_stager=None, initial=True):
        """Creates a job.

        If the job identifier is not yet used a new job will be created. The
        configuration of the job is stored in a
        :class:`webarchiver.server.job.StagerServerJob` object.

        Args:
            settings (:obj:`webarchiver.job.settings.JobSettings`): The job
                configuration.
            initial_stager (:obj:`webarchiver.server.base.Node`, optional): The
                stager server that initially started the job. If set to None
                and ``initial`` to True, this stager server will be used as
                ``initial_stager``. Default is None.
            initial (bool, optional): True if the current stager server is the
                initial stager server, else False.

        Returns:
            bool: True is the job is succesfully created.
        """
        if settings.identifier in self._jobs:
            return None
        if initial is True and initial_stager is None:
            initial_stager = self._socket
        self._jobs[settings.identifier] = StagerServerJob(settings, initial,
                                                          initial_stager)
        if initial:
            self.job_add_stager(settings.identifier)
        self.job_add_crawler(settings.identifier)
        return True

    def job_add_stager(self, identifier, listeners=None, initial=True):
        """Adds a stager server to a job.

        If a list of listeners is given these listeners will have the job
        assigned to them. Else a random number of stager server will be chosen
        to fill the number of stager servers that can be connected to the job.
        The jobs is announced to the stager servers::

            NEW_JOB <job configuration>

        If this is the initial time the job is shared among stager server, a
        list of listeners of the assigned stager servers is send to each
        assigned stager server::

            NEW_JOB_STAGER <job identifier> <listener this stager server>
            <listeners of assigned stager server>

        A counter for the URL quota for the job is randomly selected and shared
        with the assigned stager servers::

            JOB_SET_COUNTER <job identifier> <listener of counter>

        If however this is not the initial stager server sharing the job, this
        functions is called with a command to add a number of stagers to a job.
        In that case the job is initially confirmed to the stager server::

            CONFIRMED_JOB 0

        Args:
            identifier (str): The job identifier.
            listeners (list of tuples, optional): List of tuples (host, port)
                of stager servers that should be connected to the job. If None
                and ``initial`` True, this stager server is taken as initial
                stager server for the job and the job will be shared among new
                stagers. Default is None.
            initial (bool, optional): Whether the this is the initial stager
                server. Default is None.

        Returns:
            bool: True if the stager is added, False if the job is not known.
        """
        if identifier not in self._jobs:
            return False
        job = self._jobs[identifier]
        if listeners is not None:
            print(self._listeners, listeners, listeners[0] in self._listeners)
            stager = [self._listeners[l] for l in listeners]
        else:
            stager = sample(self._stager, max(0, MAX_STAGER - len(job.stagers)))
        for s in stager:
            job.add_stager(s)
            #job['stager'][s] = {
            #    'confirmed': False,
            #    'started': False
            #}
            #job['backup'][self._stager[s]['listener']] = set()
        self._write_socket_message(stager, 'NEW_JOB', job.settings)
        if initial:
            for s in job.stagers:
                self._write_socket_message(s, 'NEW_JOB_STAGER', identifier,
                                           self._address,
                                           *[d.listener for d in job.stagers
                                             if d != s])
            counter = sample(job.stagers, 1)[0]
            self._write_socket_message(job.stagers, 'JOB_SET_COUNTER',
                                       identifier, counter.listener)
            job.add_counter(counter)
        else:
            self._write_socket_message(job.stagers, 'CONFIRMED_JOB', 0,
                                       identifier)
        return True

    def job_add_crawler(self, identifier):
        """Adds a crawler server to a job.

        Every crawler server connected to the stager server is added to the
        job. Each crawler server is send the message::

            NEW_JOB_CRAWL <job configuration>

        Args:
            identifier (str): The job identifier.

        Returns:
            bool: True if the stagers are added, False is the job is not known.
        """
        if identifier not in self._jobs:
            return False
        job = self._jobs[identifier]
        for s in self._crawlers:
            job.add_crawler(s)
        self._write_socket_message(self._crawlers, 'NEW_JOB_CRAWL',
                                   job.settings)
        return True

    def start_job(self, identifier):
        """Starts a job.

        A message is send to every stager server connected to a job to start
        the job::

            JOB_START <job identifier>

        Args:
            identifier (str): The job identifier

        Returns:
            bool: True if the messages to start the job are sent, False if the
                the job is not known.
        """#TODO start job on current stager server?
        if identifier not in self._jobs:
            return False
        job = self._jobs[identifier]
        if job.started or job.finished:
            return None
        self._write_socket_message(job.stagers, 'JOB_START', identifier)
        return True

    def add_stager(self, s, listener):
        """Adds a stager server.

        If the listener of the stager server to add is not yet added, the
        stager server is added. The configuration is kept in a
        :class:`webarchiver.server.node.StagerNodeStager` object. The listener
        is added to a listeners dict pointing to the node of the stager server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server to add.
            listener (tuple): A tuple (host, port) of the stager server to add.

        Returns:
            bool: True if the stager server is added.
        """
        if listener in self._listeners:
            return None
        self._stager[s] = StagerNodeStager()
        self._listeners[listener] = s
        return True

    def add_crawler(self, s, listener):
        """Adds a crawler server.

        If the listener of the crawler server to add it not yet added, the
        crawler server is added. The configuration is kept in a
        :class:`webarchiver.server.node.StagerNodeCrawler` object. The listener
        is added to a listeners dict pointing to the node of the stager server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server to add.
            listener (tuple): A tuple (host, port) of the crawler server to
                add.

        Returns:
            bool: True if the crawler server is added.
        """
        if listener in self._listeners:
            return None
        if s in self._crawlers: #TODO is this right? (see two lines up)
            self._write_socket_message(s, 'ALREADY_CONFIRMED')
            return None
        self._crawlers[s] = StagerNodeCrawler()
        self._listeners[listener] = s
        return True

    def _command_pong(self, s, message):
        """Processes the ``PONG`` command.

        Sends a ping if this is a faulty pong message, else sets the stager
        stager server to having send a pong.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager or crawler
                server that queued the command.
            message (list): The command that was received::

                    PONG
        """
        d = self._stager if s in self._stager else self._crawlers
        if not d[s].pong:
            d[s].pong = True
        else:
            self.ping()

    def _command_job_crawl_confirmed(self, s, message):
        """Processes the ``JOB_CRAWL_CONFIRMED`` command.

        Confirmation from the crawler server that the job is added.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    JOB_CRAWL_CONFIRMED <job identifier>
        """
        if message[1] not in self._jobs:
            pass # TODO
        self._jobs[message[1]].crawler_confirmed(s)

    def _command_job_start(self, s, message):
        """Processes the ``JOB_START`` command.

        Starts the job with the given identifier. A message is send to each
        crawler server to start the job::

            JOB_START_CRAWL <job identifier>

        Note:
            This function does not yet set the variable in the job
            configuration that the job has started. This is set when all
            crawlers have started the job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    JOB_START <job identifier>
        """
        self._write_socket_message(self._jobs[message[1]].crawlers,
                                   'JOB_START_CRAWL', message[1])

    def _command_job_started_stager(self, s, message):
        """Processes the ``JOB_STARTED_STAGER`` command.

        The stager server has started the job. The job is now running on all
        crawler server that are attached to this stager server and are
        assigned to the job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    JOB_STARTED_STAGER <job identifier>
        """
        self._jobs[message[1]].started_stager(s)

    def _command_job_started_crawl(self, s, message):
        """Processes the ``JOB_STARTED_CRAWL`` command.

        The crawlers server has started the job. If the job is now fully
        started with all crawler servers on this stager server assigned to the
        job a message is send to all stager server that the job is started::

            JOB_STARTED_STAGER <job identifier>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    JOB_STARTED_CRAWL <job identifier>
        """
        job = self._jobs[message[1]]
        job.started_crawl(s)
        if job.started_local_crawl:
            self._write_socket_message(job.stagers, 'JOB_STARTED_STAGER',
                                       message[1])

    def _command_job_url(self, s, message):
        """Processes the ``JOB_URL`` command.

        Adds an URL to a job by sending it to one of the crawler servers::

            JOB_URL_CRAWL :obj:`webarchiver.url.UrlConfig`

        Note:
            The URL is not backed up here, because a backup command is already
            send to other stager servers when this command was send to this
            server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    JOB_URL :obj:`webarchiver.url.UrlConfig`
        """
        crawler = self._jobs[message[1].job_identifier] \
            .add_url_crawler(message[1])
        self._write_socket_message(crawler, 'JOB_URL_CRAWL', message[1])

    def _command_job_url_backup(self, s, message):
        """Processes the ``JOB_URL_BACKUP`` command.

        Backs up an URL that was assigned to another stager server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    JOB_URL_BACKUP :obj:`webarchiver.url.UrlConfig`
                        <listener assigned server>
        """
        #self._jobs[message[1]]['urls'].add(message[2]) #TODO should this be currently crawling or currently using
        self._jobs[message[1].job_identifier] \
            .backup_url(self._listeners[message[2]], message[1])

    def _command_job_url_finished(self, s, message):
        """Processes the ``JOB_URL_FINISHED`` command.

        The URL assigned to a crawler server is finished. This is reported to
        the job on this stager server.

        Note:
            This command is send to every stager server connected to a crawler
            server to make sure both the queueing stager server and those
            with a backup remove the URL.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    JOB_URL_FINISHED <job_identifier> <URL>
                        <listener that queued URL>
        """ #TODO: the crawler server is not connected to every stager server. should the URL first be send to the stager server that queued it, which sends it to all stager server?
        self._jobs[message[1]].finish_url(s, message[2], message[3])

    def _command_job_url_discovered(self, s, message):
        """Processes the ``JOB_URL_DISCOVERED`` command.

        Adds an URL discovered by the crawler server to the job to be shared
        among other stager server again later.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    JOB_URL_DISCOVERED :obj:`webarchiver.url.UrlConfig`
        """
        # TODO check if URL should be crawled
        self._jobs[message[1].job_identifier].add_url(message[1])

    def _command_job_set_counter(self, s, message):
        """Processes the ``JOB_SET_COUNTER`` command.

        Set the specified stager server as counter for the given job
        identifier. If the listener is that of this stager server, this stager
        server makes itself the counter for the URL quota.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    JOB_SET_COUNTER <job identifier> <listener of counter>
        """
        job = self._jobs[message[1]]
        if message[2] == self._address:
            job.set_as_counter()
        else:
            job.add_counter(self._listeners[message[2]])

    def _command_request_stager(self, s, message):
        """Processes the ``REQUEST_STAGER`` command.

        Send back a number of stager server to the crawler server that is
        requesting them. Each stager server is send with::

            ADD_STAGER <listener of the stager server>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    REQUEST_STAGER <number stagers needed>
                        <listeners of connected stager servers>
        """
        listeners = message[2:]
        for s_ in sample(self._stager, message[1]):
            if s_.listener not in listeners:
                self._write_socket_message(s, 'ADD_STAGER',
                                           s_.listener)

    def _command_request_url_quota(self, s, message):
        """Processes the ``REQUEST_URL_QUOTA`` command.

        The crawler server has requested a quota for the number of URLs it is
        allowed to archive. If this stager server is the counter for the quota,
        this goes to function :func:`_command_assigned_url_quota_crawler`
        belonging with command ``ASSIGNED_URL_QUOTA_CRAWLER``, where the quota
        is send back.

        If the stager server is not the counter a request for URL quota is send
        to the stager server that is the counter::

            REQUEST_URL_QUOTA_CRAWLER <job identifier>
                <listener crawler server>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    REQUEST_URL_QUOTA <job identifier>
        """
        print(s.listener)
        job = self._jobs[message[1]]
        if job.is_counter:
            self._command_assigned_url_quota_crawler(None,
                ['', message[1], job.url_quota, s.listener])
        else:
            self._write_socket_message(job.counter,
                                       'REQUEST_URL_QUOTA_CRAWLER', message[1],
                                       s.listener)

    def _command_request_url_quota_crawler(self, s, message):
        """Processes the ``REQUEST_URL_QUOTA_CRAWLER`` command.

        The stager server requests an URL quota for a crawler server. This
        stager server is the counter for this URL quota. The URL quota is taken
        and send back::

            ASSIGNED_URL_QUOTA_CRAWLER <job identifier> <assigned quota>
                <listener crawler server>

        Note:
            This command can never be called from a crawler server. It is only
            called from a stager server if it got a request for an URL quota,
            but it is not the counter of the URL quota.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    REQUEST_URL_QUOTA_CRAWLER <job identifier>
                        <listener crawler server>
        """
        quota = self._jobs[message[1]].url_quota
        self._write_socket_message(s, 'ASSIGNED_URL_QUOTA_CRAWLER',
                                   message[1], quota, *message[2:])

    def _command_assigned_url_quota_crawler(self, s, message):
        """Processes the ``ASSIGNED_URL_QUOTA_CRAWLER`` command.

        An URL quota is assigned to the crawler server. This is send back to
        the crawler server::

            ASSIGNED_URL_QUOTA <job identifier> <assigned quota>

        Note:
            This function can be called directly from this stager server if it
            is the counter for the URL quota.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    ASSIGNED_URL_QUOTA_CRAWLER <job identifier>
                        <assigned quota> <listener crawler server>
        """
        self._write_socket_message(self._listeners[message[3]],
                                   'ASSIGNED_URL_QUOTA', *message[1:3])
                                   

#    def _command_stager_added(self, s, message):
#        pass

#    def _command_stager_already_added(self, s, message):
#        pass

    def _command_new_job(self, s, message):
        """Processes the ``NEW_JOB`` command.

        A new job will be added to this stager server. The stager server
        sending this command is taken as the initial stager server starting the
        job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    NEW_JOB <job configuration>
        """
        self.create_job(message[1], initial_stager=s, initial=False)

    def _command_new_job_stager(self, s, message):
        """Processes the ``NEW_JOB_STAGER`` command.

        Adds the stager assigned to the given job to this job. This includes
        the stager server that initially created this job.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    NEW_JOB_STAGER <job identifier>
                        <listeners of assigned stager server>
        """
        self.job_add_stager(message[1], listeners=message[2:], initial=False)

    def _command_confirmed_job(self, s, message):
        """Processes the ``CONFIRMED_JOB`` command.

        The stager server to which the list of stager server was send that are
        assigned to the job reports that the job is confirmed and ready to
        start. A confirmation is send back depending on the state of the
        confirmation::

            CONFIRMED_JOB 1

        If the job was not found a confirmation with error is send back::

            CONFIRMED_JOB -1

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    CONFIRMED_JOB <state>
        """#TODO do something with error confirmation
        if message[2] not in self._jobs:
            self._write_socket_message(s, message[0], -1, message[2])
            return None
        i = self._jobs[message[2]].confirmed(s, message[1])
        if i != -1 and i != None:
            self._write_socket_message(s, 'CONFIRMED_JOB', i, message[2])

    def _command_announce_crawler(self, s, message):
        """Processes the ``ANNOUNCE_CRAWLER`` command.

        A crawler server announces itself using its listener. This stager
        server creates a connection with the crawler. It sends back a
        confirmation::

            CONFIRMED 0

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    ANNOUNCE_CRAWLER <crawler listener>
        """
        s.listener = message[1]
        self.add_crawler(s, message[1])
        self._write_socket_message(s, 'CONFIRMED', 0)

    def _command_announce_crawler_extra(self, s, message):
        """Processes the ``ANNOUNCE_CRAWLER_EXTRA`` command.

        A crawler server announces itself using its listener. This stager
        server is not the initial stager server the crawler server connects to.

        Note:
            Currently the same behavior as for a received ``ANNOUNCE_CRAWLER``
            command.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    ANNOUNCE_CRAWLER_EXTRA <crawler listener>
        """
        self._command_announce_crawler(s, message)

    def _command_announce_stager(self, s, message, extra=False):
        """Processes the ``ANNOUNCE_STAGER`` command.

        A stager server announces itself using its listener. This stager server
        creates a connection using the listener. It sends back a confirmation::

            CONFIRMED 0

        If this is the first stager server the announcing stager server
        announces itself to, each stager server that is currently connected
        will be send using its listener::

            STAGER_NEW <stager listener>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    ANNOUNCE_STAGER <stager listener>
        """
        s.listener = message[1]
        if self.add_stager(s, message[1]) and not extra:
            for s_ in self._stager:
                if s_ == s:
                    continue
                self._write_socket_message(s, 'STAGER_NEW', s_.listener)
        self._write_socket_message(s, 'CONFIRMED', 0)

    def _command_announce_stager_extra(self, s, message):
        """Processes the ``ANNOUNCE_STAGER_EXTRA`` command.

        A stager server announces itself using its listener. This stager server
        creates a connection using the listener. This is not the first
        connection the stager server makes.

        Note:
            The behavior is different from ``ANNOUNCE_STAGER`` in that not
            every connected stager server is send using its listener.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    ANNOUNCE_STAGER_EXTRA <stager listener>
        """
        self._command_announce_stager(s, message, extra=True)

    def _command_stager_new(self, s, message):
        """Processes the ``STAGER_NEW`` command.

        Adds a stager server received from the initial connected stager server.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    STAGER_NEW <stager listener>
        """
        self.init_stager(message[1], extra=True)

    def _command_confirmed(self, s, message):
        """Processes the ``CONFIRMED`` command.

        A confirmation to this stager server by another server. This stager
        server confirms back if the state of the confirmation is 0::

            CONFIRMED 1

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager or crawler
                server that queued the command.
            message (list): The command that was received::

                    CONFIRMED <state>
        """#TODO unconsistent behavior on initial confirmation and confirming back.
        d = self._stager if s in self._stager else self._crawlers
        if not d[s].confirmed:
            d[s].confirmed = True
            if message[1] == 0:
                self._write_socket_message(s, 'CONFIRMED', 1)

    def _command_request_upload_permission(self, s, message):
        """Processes the ``REQUEST_UPLOAD_PERMISSION`` command.

        A request for permission to upload a file to this stager server. If
        there is enough free disk space for the file, the permission is
        granted::

            UPLOAD_PERMISSION_GRANTED <job identifier> <WARC path>

        If the file is too large, upload permission is not granted::

            UPLOAD_PERMISSION_DENIED <job identifier> <WARC path>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    REQUEST_UPLOAD_PERMISSION <job identifier> <WARC path>
                        <WARC filesize>
        """
        size = int(message[3])
        if self.free_space > size:
            self._uploading[message[2]] = size
            self.free_space -= size
            self._write_socket_message(s, 'UPLOAD_PERMISSION_GRANTED',
                                       *message[1:3])
        else:
            self._write_socket_message(s, 'UPLOAD_PERMISSION_DENIED',
                                       *message[1:3])

    def _command_request_upload_revoke(self, s, message):
        """Processes the ``REQUEST_UPLOAD_REVOKE`` command.

        If a permission for upload is not used anymore, the request from a
        crawler server to a stager server is send to revoke the permission,
        clearing space for new upload requests.

        Args:
            s (:obj:`webarchiver.server.base.Node`): The crawler server that
                queued the command.
            message (list): The command that was received::

                    REQUEST_UPLOAD_REVOKE <job identifier> <WARC path>
        """#TODO some possibly strange behavior with double revokes
        if message[2] not in self._uploading:
            return None
        self.free_space += self._uploading[message[2]]
        del self._uploading[message[2]]

    def _command_warc_file(self, s, message):
        """Processes the ``JOB_URL`` command.

        Saves a received WARC file to disk and confirms the file was received::

            WARC_FILE_RECEIVED <job identifier> <path>

        Args:
            s (:obj:`webarchiver.server.base.Node`): The stager server that
                queued the command.
            message (list): The command that was received::

                    WARC_FILE <path> <file> <job identifier>
        """
        if message[3] not in self._jobs:
            return None
        path = os.path.join('warc', message[3], os.path.basename(message[1]))
        if write_file(path, message[2]):
            self._write_socket_message(s, 'WARC_FILE_RECEIVED', message[3],
                message[1])

    @property
    def free_space(self):
        """int: The available space. ``MAX_SPACE`` is maximum available space.
        The available space is at minimum 0.
        """
        r = MAX_SPACE - self._used_space
        return r if r >= 0 else 0

    @free_space.setter
    def free_space(self, value):
        self._used_space += self.free_space - value

