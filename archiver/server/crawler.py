import functools
import os
import random
import socket
import time

from archiver.config import *
from archiver.database import UrlDeduplicationDatabase
from archiver.server.base import BaseServer, Node
from archiver.server.job import CrawlerServerJob
from archiver.server.node import CrawlerNode
from archiver.utils import check_time, get_listener, key_lowest_value, sample


class CrawlerServer(BaseServer):
    def __init__(self, ip=None, port=None):
        self._port = random.randrange(3000, 6000)
        super().__init__(socket.getfqdn(), self._port)
        self._staging = {}
        self.add_staging((ip, port))
        self._jobs = {}
        self._upload_permissions = UploadPermissions()
        self._filenames_set = set()
        self._finished_urls_set = set()
        self._found_urls_set = set()
        self._last_upload_request = 0
        self._last_url_quota = 0

    def _run_round(self):
        super()._run_round()
        self.request_staging()
        self.ping()
        self.upload()
        self.finish_urls()
        self.found_urls()
        self.request_url_quota()

    def _create_socket(self, address=None):
        s = self._connect_socket(address or self._address)
        self._write_queue[s] = []
        return s

    def add_staging(self, listener, extra=False, s=None):
        for s_ in self._staging:
            if s_.listener == listener:
                return None
        s_ = self._create_socket(listener) if not s else s
        self._read_list.append(s_)
        self._staging[s_] = CrawlerNode()
        self._write_socket_message(s_, 'ANNOUNCE_CRAWLER' \
                                   + ('_EXTRA' if extra else ''),
                                   *self._address)
        return True

    def request_staging(self):
        if self.staging_needed > 0 and check_time(self._last_staging_request,
                                                  REQUEST_STAGING_TIME):
            message = []
            for s in self._staging:
                message.extend(s.listener)
            self._write_socket_message(sample(self._staging, 1),
                                       'REQUEST_STAGING', self.staging_needed,
                                       *message)
            self._last_staging_request = time.time()

    def ping(self):
        if check_time(self._last_ping, PING_TIME):
            for s in self._staging:
                self._staging[s].pong = False
            self._write_socket_message(self._staging, 'PING')
            self._last_ping = time.time()

  #  def request_upload(self):
  #      if check_time(self._last_upload_request, config.REQUEST_UPLOAD_TIME):
  #          self._write_socket_message(self._jobs[job]['staging'],
  #                                     'REQUEST_UPLOAD')
  #          self._last_upload_request = time.time()
  #          time.sleep(1)

    def upload(self):
        for job, path in self._filenames_set:
            warc_file = self._upload_permissions[path]
            if not warc_file.requested:
                self._write_socket_message(self._jobs[job].staging,
                                           'REQUEST_UPLOAD_PERMISSION', job,
                                           path, warc_file.filesize)
                warc_file.requested = True
                continue
            if warc_file.chosen is False:
                del self._upload_permissions[path]
            elif warc_file.chosen is not None and not warc_file.revoked:
                self._write_socket_message(warc_file.to_revoke,
                                           'REQUEST_UPLOAD_REVOKE', job, path)
                warc_file.revoked = True
                self.upload_warc(warc_file.chosen, job, path)

    def upload_warc(self, s, job, path):
        if os.path.isfile(path + '.uploading'):
            return False
        open(path + '.uploading', 'w').close()
        self._write_socket_file(s, path, 'WARC_FILE', job)
        return True #TODO confirmation?

    def finish_urls(self):
        finished = set()
        for identifier, url in self._finished_urls_set:
            print(self._jobs)
            #print(identifier, url)
            job = self._jobs[identifier]
            job.finished_url(url)
            self._write_socket_message(job.staging, 'JOB_URL_FINISHED',
                                       identifier, url,
                                       *job.get_url_staging(url).listener)
            finished.add((identifier, url))
            job.delete_url_staging(url)
            print(self._jobs)
        self._finished_urls_set.difference_update(finished)

    def found_urls(self):
        finished = set()
        for identifier, parenturl, url in self._found_urls_set:
            finished.add((identifier, parenturl, url))
            if self._jobs[identifier].archived_url(url):
                continue
            staging = sample(self._jobs[identifier].staging, 1)[0]
            self._write_socket_message(staging, 'JOB_URL_DISCOVERED',
                                       identifier, parenturl, url)
        self._found_urls_set.difference_update(finished)

    def request_url_quota(self):
        if check_time(self._last_url_quota, URL_QUOTA_TIME):
            self._last_url_quota = time.time()
            if len(self._jobs) == 0:
                return None
            job = key_lowest_value({
                job: self._jobs[job].received_url_quota
                for job in self._jobs
            })
            self._write_socket_message(sample(self._jobs[job].staging, 1)[0],
                                       'REQUEST_URL_QUOTA', job)

    def create_job(self, identifier):
        if identifier in self._jobs:
            return None
        self._jobs[identifier] = CrawlerServerJob(identifier,
                                                  self._filenames_set,
                                                  self._finished_urls_set,
                                                  self._found_urls_set)

    def start_job(self, identifier):
        if identifier not in self._jobs:
            return False
        return self._jobs[identifier].start()

    def job_add_staging(self, identifier, s):
        if identifier not in self._jobs:
            return False
        self._jobs[identifier].add_staging(s)
        return True

    def job_add_url(self, s, identifier, url):
        if identifier not in self._jobs:
            return False
        self._jobs[identifier].add_url(s, url)
        return True

    def _command_pong(self, s, message):
        if self._staging[s].pong is False:
            self._staging[s].pong = True
        else:
            self.ping()

    def _command_confirmed(self, s, message):
        self._staging[s].confirmed = True
        if message[1] == '0':
            self._write_socket_message(s, 'CONFIRMED', 1)

    def _command_assigned_url_quota(self, s, message):
        self._jobs[message[1]].increase_url_quota(int(message[2]))

    def _command_new_job_crawl(self, s, message):
        self.create_job(message[1])
        self.job_add_staging(message[1], s)
        self._write_socket_message(s, 'JOB_CRAWL_CONFIRMED', message[1])

    def _command_job_url_crawl(self, s, message):
        self.job_add_url(s, message[1], message[2])

    def _command_job_start_crawl(self, s, message):
        self.start_job(message[1])
        self._write_socket_message(s, 'JOB_STARTED_CRAWL', message[1])

#    def _command_upload_requested(self, s, message):
#        self._jobs[s]['upload'] = eval(message[2]) #FIXME

    def _command_warc_file_received(self, s, message):
        self._filenames_set.remove((message[1], message[2]))
        del self._upload_permissions[message[2]]
        os.remove(message[2])
        os.remove(message[2] + '.uploading')

    def _command_add_staging(self, s, message):
        listener = get_listener(message[1:])[0]
        r = self.add_staging(listener, extra=True)
#        if r is None: TODO don't have to report this(?)
#            self._write_socket_message(s, 'STAGING_ALREADY_ADDED', *listener)
#        elif r is true:
#            self._write_socket_message(s, 'STAGING_ADDED', *listener)

    def _command_upload_permission_granted(self, s, message):
        warc_file = self._upload_permissions[message[2]]
        if not warc_file.requested:
            return False
        warc_file.granted(s)

    def _command_upload_permission_denied(self, s, message):
        pass

    def _command_already_confirmed(self, s, message):
        pass # TODO

    @property
    def staging_needed(self):
        n = MAX_STAGING - len(self._staging)
        return 0 if n < 0 else n


class UploadPermissions(dict):
    def __init__(self):
        super().__init__(self)

    def __getitem__(self, path):
        if path not in self:
            super().__setitem__(path, WarcFile(path))
        return super().__getitem__(path)


class WarcFile:
    def __init__(self, path):
        self.requested = False
        self.revoked = False
        self._granted = []
        self._last_answer = 0
        self._path = path

    def granted(self, s):
        self._granted.append(s)
        self._last_answer = time.time()

    @property
    def to_revoke(self):
        return [s for s in self._granted if s != self.chosen]

    @property
    @functools.lru_cache()
    def filesize(self):
        return os.path.getsize(self._path)

    @property
    def chosen(self):
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

