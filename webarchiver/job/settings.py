import os
import sys

from webarchiver.request import get


class JobSettingsException(LookupError):
    pass


class JobSettings:
    def __init__(self, identifier, config, location):
        self.identifier = identifier
        self.config = config
        self.regex = tuple(self.config['regex'].split('\n'))
        self._add_setting('rate', sys.maxsize)
        self._add_setting('depth', sys.maxsize)
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

    def _add_setting(self, key, default):
        setattr(self, key, self.config[key] if key in self.config else default)

    def get_raw_response(self, url):
        return self._raw_responses.get(url)

