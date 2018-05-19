import os
import sys

from webarchiver.request import get
from webarchiver.utils import random_string


class JobSettingsException(LookupError):
    pass


class JobSettings:
    def __init__(self, identifier, config, location):
        self.identifier = '{}_{}'.format(identifier, random_string(8))
        self.config = config
        self.allow_regex = tuple(self.config['allow regex'].split('\n'))
        self.ignore_regex = tuple(self.config['ignore regex'].split('\n'))
        self._add_setting('rate', sys.maxsize, int)
        self._add_setting('depth', sys.maxsize, int)
        self._raw_responses = {}
        self._raw_files = {}
        with open(location, 'rb') as f:
            self._raw_config = f.read()
        urls = list(self.config['url'].split('\n')) \
            if 'url' in self.config else []
        if 'urls file' in self.config:
            for path in self.config['urls file'].split('\n'):
                with open(os.path.join(os.path.dirname(location), path)) as f:
                    for url in f:
                        url = url.strip()
                        if len(url) != 0:
                            urls.append(url)
        if 'urls url' in self.config:
            for url in self.config['urls url'].split('\n'):
                r = get(url)
                if not r:
                    raise JobSettingsException('Url {} not available.'
                                               .format(url))
                self._raw_responses[url] = r
                for url in r.text.splitlines():
                    url = url.strip()
                    if len(url) != 0:
                        urls.append(url)
        self.urls = tuple(urls)

    def _add_setting(self, key, default, t):
        setattr(self, key, t(self.config[key])
                if key in self.config else default)

    def get_raw_response(self, url):
        return self._raw_responses.get(url)

