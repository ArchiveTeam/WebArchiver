def init_urls(job_identifier, urls):
    return {url: UrlConfig(job_identifier, url, 0, None) for url in urls}


class UrlConfig:
    def __init__(self, job_identifier, url, depth, parent_url):
        self.job_identifier = job_identifier
        self.url = url
        self.depth = depth
        self.parent_url = parent_url

