import os
import random
import socket
import time

from archiver.config import *
from archiver.server.job import StagerServerJob
from archiver.server.base import BaseServer, Node
from archiver.server.node import StagerNodeCrawler, StagerNodeStager
from archiver.utils import check_time, sample, write_file


class StagerServer(BaseServer):
    def __init__(self, ip=None, port=None):
        self._port = random.randrange(3000, 6000)
        super().__init__(socket.getfqdn(), self._port)
        self._data_sockets = []
        self._urls = None#Urls()
        self._stager = {}
        self._listeners = {}
        self._crawlers = {}
        if ip is not None and port is not None:
            self.init_stager(self._connect_socket((ip, port)), (ip, port))
        self._jobs = {}
        self._last_jobs_check = 0
        self._used_space = 0
        self._uploading = {}
        self.test = 0

    def _run_round(self):
        super()._run_round()
        self.ping()
        if os.path.isfile('starttest' + str(self._port)) and len(self._jobs) == 0:
            with open('starttest' + str(self._port), 'r') as f:
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

    def _read_socket(self, s):
        if s == self._socket:
            server, address = self._socket.accept()
            server = Node(server)
            self._read_list.append(server)
            self._write_queue[server] = []
        else:
            super()._read_socket(s)

    def init_stager(self, s, listener, extra=False):
        if listener in self._listeners:
            return None
        self._write_queue[s] = []
        self._read_list.append(s)
        #print(s)
        self.add_stager(s, listener)
        self._write_socket_message(s, 'ANNOUNCE_STAGER' \
                                   + ('_EXTRA' if extra else ''),
                                   self._address)
        return True

    def ping(self):
        if check_time(self._last_ping, PING_TIME):
            for s in self._stager:
                self._stager[s].pong = False
            self._write_socket_message(self._stager, 'PING')
            for s in self._crawlers:
                self._crawlers[s].pong = False
            self._write_socket_message(self._crawlers, 'PING')
            self._last_ping = time.time()

    def check_jobs(self):
        if check_time(self._last_jobs_check, JOBS_CHECK_TIME):
            for job in self._jobs.values():
                for url, s, backups in job.share_urls():
                    if s is None:
                        s = self._socket
                        self._command_job_url(None, [None, job.identifier,
                                                     url])
                    else:
                        self._write_socket_message(s, 'JOB_URL',
                                                   job.identifier, url)
                    self._write_socket_message(backups, 'JOB_URL_BACKUP',
                                               job.identifier, url, s.listener)
            self._last_jobs_check = time.time()

    def _command_job_url(self, s, message):
        crawler = self._jobs[message[1]].add_url_crawler(message[2])
        self._write_socket_message(crawler, 'JOB_URL_CRAWL', message[1],
                                   message[2])

    def create_job(self, identifier, urls=set(), initial_stager=None,
                   initial=True):
        if identifier in self._jobs:
            return None
        if initial is True and initial_stager is None:
            initial_stager = self._socket
        self._jobs[identifier] = StagerServerJob(identifier, urls,
                                                  initial_stager)
        if initial:
            self.job_add_stager(identifier)
        self.job_add_crawler(identifier)
        return True

    def job_add_stager(self, identifier, listeners=None, initial=True):
        if identifier not in self._jobs:
            return False
        job = self._jobs[identifier]
        if listeners is not None:
            print(self._listeners, listeners, listeners[0] in self._listeners)
            stager = [self._listeners[l] for l in listeners]
        else:
            stager = sample(self._stager, max(0, MAX_STAGER - len(job.stager)))
        for s in stager:
            job.add_stager(s)
            #job['stager'][s] = {
            #    'confirmed': False,
            #    'started': False
            #}
            #job['backup'][self._stager[s]['listener']] = set()
        self._write_socket_message(stager, 'NEW_JOB', identifier)
        if initial:
            for s in job.stager:
                self._write_socket_message(s, 'NEW_JOB_STAGER', identifier,
                                           self._address,
                                           *[d for d in job.stager if d != s])
            counter = sample(job.stager, 1)[0]
            self._write_socket_message(job.stager, 'JOB_SET_COUNTER',
                                       identifier, counter.listener)
            job.add_counter(counter)
        else:
            self._write_socket_message(job.stager, 'CONFIRMED_JOB', 0,
                                       identifier)
        return True

    def job_add_crawler(self, identifier):
        if identifier not in self._jobs:
            return False
        for s in self._crawlers:
            self._jobs[identifier].add_crawler(s)
        self._write_socket_message(self._crawlers, 'NEW_JOB_CRAWL', identifier)
        return True

    def start_job(self, identifier):
        if identifier not in self._jobs:
            return False
        job = self._jobs[identifier]
        if job.started or job.finished:
            return None
        self._write_socket_message(job.stager, 'JOB_START', identifier)
        return True

    def add_stager(self, s, listener):
        if listener in self._listeners:
            return None
        self._stager[s] = StagerNodeStager()
        self._listeners[listener] = s
        return True

    def add_crawler(self, s, listener):
        if listener in self._listeners:
            return None
        if s in self._crawlers:
            self._write_socket_message(s, 'ALREADY_CONFIRMED')
            return None
        self._crawlers[s] = StagerNodeCrawler()
        self._listeners[listener] = s
        return True

    def _command_pong(self, s, message):
        d = self._stager if s in self._stager else self._crawlers
        if not d[s].pong:
            d[s].pong = True
        else:
            self.ping()

    def _command_job_crawl_confirmed(self, s, message):
        if message[1] not in self._jobs:
            pass # TODO
        self._jobs[message[1]].crawler_confirmed(s)

    def _command_job_start(self, s, message):
        self._write_socket_message(self._jobs[message[1]].crawlers,
                                   'JOB_START_CRAWL', message[1])

    def _command_job_started_stager(self, s, message):
        self._jobs[message[1]].started_stager(s)

    def _command_job_started_crawl(self, s, message):
        job = self._jobs[message[1]]
        job.started_crawl(s)
        if job.started_local_crawl:
            self._write_socket_message(job.stager, 'JOB_STARTED_STAGER',
                                       message[1])

    def _command_job_url(self, s, message):
        crawler = self._jobs[message[1]].add_url_crawler(message[2])
        self._write_socket_message(crawler, 'JOB_URL_CRAWL', message[1],
                                   message[2])

    def _command_job_url_backup(self, s, message):
        #self._jobs[message[1]]['urls'].add(message[2]) #TODO should this be currently crawling or currently using
        self._jobs[message[1]].backup_url(s, message[2])

    def _command_job_url_finished(self, s, message):
        self._jobs[message[1]].finish_url(s, message[2], message[3])

    def _command_job_url_discovered(self, s, message):
        # TODO check if URL should be crawled
        self._jobs[message[1]].add_url(message[3])

    def _command_job_set_counter(self, s, message):
        job = self._jobs[message[1]]
        if message[2] == self._address:
            job.set_as_counter()
        else:
            job.add_counter(self._listeners[message[2]])

    def _command_request_stager(self, s, message):
        listeners = message[2:]
        for s_ in sample(self._stager, message[1]):
            print(type(s_))
            if s_.listener not in listeners:
                self._write_socket_message(s, 'ADD_STAGER',
                                           s_.listener)

    def _command_request_url_quota(self, s, message):
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
        quota = self._jobs[message[1]].url_quota
        self._write_socket_message(s, 'ASSIGNED_URL_QUOTA_CRAWLER',
                                   message[1], quota, *message[2:])

    def _command_assigned_url_quota_crawler(self, s, message):
        self._write_socket_message(self._listeners[message[3]],
                                   'ASSIGNED_URL_QUOTA', *message[1:3])
                                   

#    def _command_stager_added(self, s, message):
#        pass

#    def _command_stager_already_added(self, s, message):
#        pass

    def _command_new_job(self, s, message):
        self.create_job(message[1], initial_stager=s, initial=False)

    def _command_new_job_stager(self, s, message):
        self.job_add_stager(message[1], listeners=message[2:], initial=False)

    def _command_confirmed_job(self, s, message):
        if message[2] not in self._jobs:
            self._write_socket_message(s, message[0], -1, message[2])
            return None
        i = self._jobs[message[2]].confirmed(s, message[1])
        if i != -1 and i != None:
            self._write_socket_message(s, 'CONFIRMED_JOB', i, message[2])

    def _command_announce_crawler(self, s, message):
        s.listener = message[1]
        self.add_crawler(s, message[1])
        self._write_socket_message(s, 'CONFIRMED', 0)

    def _command_announce_crawler_extra(self, s, message):
        self._command_announce_crawler(s, message)

    def _command_announce_stager(self, s, message, extra=False):
        s.listener = message[1]
        if self.add_stager(s, message[1]) and not extra:
            for s_ in self._stager:
                if s_ == s:
                    continue
                self._write_socket_message(s, 'STAGER_NEW', s_.listener)
        self._write_socket_message(s, 'CONFIRMED', 0)

    def _command_announce_stager_extra(self, s, message):
        self._command_announce_stager(s, message, extra=True)

    def _command_stager_new(self, s, message):
        self.init_stager(self._connect_socket(message[1]), message[1],
                         extra=True)

    def _command_confirmed(self, s, message):
        d = self._stager if s in self._stager else self._crawlers
        if not d[s].confirmed:
            d[s].confirmed = True
            if message[1] == 0:
                self._write_socket_message(s, 'CONFIRMED', 1)

    def _command_request_upload_permission(self, s, message):
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
        if message[2] not in self._uploading:
            return None
        self.free_space += self._uploading[message[2]]
        del self._uploading[message[2]]

    def _command_warc_file(self, s, message):
        if message[3] not in self._jobs:
            return None
        path = os.path.join('warc', message[3], os.path.basename(message[1]))
        if write_file(path, message[2]):
            self._write_socket_message(s, 'WARC_FILE_RECEIVED', message[3],
                message[1])

    @property
    def free_space(self):
        r = MAX_SPACE - self._used_space
        return r if r >= 0 else 0

    @free_space.setter
    def free_space(self, value):
        self._used_space += self.free_space - value

