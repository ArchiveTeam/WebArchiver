"""URL configuration and processing."""


def init_urls(job_identifier, urls):
    """Creates a dict of :class:`UrlConfig` objects for the given URLs.

    Args:
        job_identifier (str): The identifier of the job the URL is for.
        urls (list or set): The URLs that the :class:`UrlConfig` objects should
            be created for.
    """
    return {url: UrlConfig(job_identifier, url, 0, None) for url in urls}


class UrlConfig:
    """The configuration for an URL.

    Attributes:
        job_identifier (str): The job identifier for the URL.
        url (str): The URL.
        depth (int): The depth of the URL in the crawl.
        parent_url (int): The URL through which this URL was found.
    """

    def __init__(self, job_identifier, url, depth, parent_url):
        """Inits the configuration for an URL.

        Args:
            job_identifier (str): The job identifier for the URL.
            url (str): The URL.
            depth (int): The depth of the URL in the crawl.
            parent_url (int): The URL through which this URL was found.
        """
        self.job_identifier = job_identifier
        self.url = url
        self.depth = depth
        self.parent_url = parent_url

    def __repr__(self):
        return '<{} at 0x{:x} job={}>' \
            .format(__name__, id(self), self.settings.identifier)

    def __hash__(self):
        return hash(';'.join(self.job_identifier, self.url, self.depth,
                             self.parent_url))

