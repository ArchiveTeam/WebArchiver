"""Manages job settings."""
import os
import sys

from webarchiver.request import get
from webarchiver.utils import random_string


class JobSettingsException(LookupError):
    """General exception class for errors in the job settings."""
    pass


class JobSettingsUrlNotAvailable(JobSettingsException):
    """Exception for an unavailable URL given in the job settings."""
    pass


class JobSettings:
    """The settings for a job.

    Attributes:
        identifier: The job identifier. This is the given identifier appended
            with ``_`` and eight random characters matching [a-z0-9].
        config: The original loaded configuration configuration file.
        allow_regex: The allowed regular expressions for URLs discovered during
            the job. Discovered URLs that do not match one or more of the
            regular expressions are not used.
        ignore_regex: The not alowed regular expressions for the URLs
            discovered during the job. URLs that match one or more of the
            regular expressions are not used.
        urls: The list of initial URLs for the job.
    """

    def __init__(self, identifier, config, location):
        """Creates the settings for the job.

        The raw responses, files and raw config file are saved in private
        variables for possible later use or checking in case of errors. Each
        file containing URLs and each webpage containing URLs is downloaded
        and added to the URLs list.

        Args:
            identifier: The job identifier. This is the given identifier
                appended with ``_`` and eight random characters matching
                [a-z0-9].
            config: The original loaded configuration configuration file.
            location: The location of the configuration file.

        Raises:
            JobSettingsUrlNotAvailable: If the URL of a webpage containing URLs
                to import is not available.
        """
        self.identifier = '{}_{}'.format(identifier, random_string(8))
        self.config = config
        self.allow_regex = tuple(self.config['allow regex'].split('\n'))
        self.ignore_regex = tuple(self.config['ignore regex'].split('\n')
                                  if 'ignore regex' in self.config else [])
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
                    raise JobSettingsUrlNotAvailable('Url {} not available.'
                                               .format(url))
                self._raw_responses[url] = r
                for url in r.text.splitlines():
                    url = url.strip()
                    if len(url) != 0:
                        urls.append(url)
        self.urls = tuple(urls)

    def _add_setting(self, key, default, t):
        """Add a certain setting with a specified default.

        Args:
            key (str): The attribute name to use for saving and getting the
                value from the configuration file.
            default: The default value to use in case the ``key`` is not in the
                configuration file.
            t (class): The type the value should have.
        """
        setattr(self, key, t(self.config[key])
                if key in self.config else default)

    def get_raw_response(self, url):
        """Gets the response for a URL.

        Args:
            url (str): The URL do GET.

        Returns:
            :obj:`requests.model.Response`: The response of GETting the URL.
        """
        return self._raw_responses.get(url)

