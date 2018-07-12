"""Configuration."""
import datetime
import os

# crawler
REQUEST_STAGER_TIME = 120
REQUEST_UPLOAD_TIME = 5
PING_TIME = 60
MAX_BACKUPS = 3
MAX_STAGER = 5
MAX_SPACE = 1000000000

LOGS_DIRECTORY = 'logs'
LOG_PATH = os.path.join(LOGS_DIRECTORY, '{}.log'
                            .format(datetime.datetime.today()
                                    .strftime('%Y%m%d%H%M%S')))

URL_QUOTA_TIME = 2

LISTEN_QUEUE = 300

NEW_JOBS_DIR = 'jobs'
JOB_MAX_URLS = 1000
JOB_MAX_WAIT = 300
JOB_MAX_WAIT_URLS = 30
JOBS_CHECK_TIME = 5
FINISH_CHECK_TIME = 60

CRAWLER_MIN_URL_QUOTA = 100
CRAWL_SCRIPTS = 'crawl'
CRAWLS_NEW_URLS_FILE = 'new_urls.txt'
CRAWLS_DIRECTORY = 'data'
WGET_EXIT_CODES = [0, 4, 6, 8]
WGET_EXECUTABLE = 'wget'
USER_AGENT = 'ArchiveTeam; Googlebot/2.1'
WGET_LOG = 'wget.log'
WGET_TEMP = 'wget.tmp'
WGET_TIMEOUT = '30'
WGET_WAITRETRY = '30'
WGET_TRIES = '5'
VERSION = '0.0.1'
DEDUPLICATION_SERVER = None
FILES = []

DEFAULT_CRAWLER_SERVER_CONFIG = {
    'confirmed': False
}

DEFAULT_CRAWLER_SERVER_CONFIG = {
    'confirmed': False
}
