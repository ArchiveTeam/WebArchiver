"""Simple extraction of URLs."""
import functools
import re
import string
import urllib.parse

printable_bytes = bytes(string.printable, 'ascii')
"""The printable bytes."""


def extract_urls(parenturl, record):
    """Parses the data in a WARC record and yield extracted URLs.

    Args:
        record (:obj:`warcio.recordloader.ArcWarcRecord`): The WARC record.

    Yields:
        str: The discovered URL.
    """
    for line in iter(record.content_stream().readline, ''):
        if line == b'':
            break
        for url in extract(bytes(parenturl, 'UTF8'), line):
            if type(url) is bytes \
                    and all(byte in printable_bytes for byte in url):
                url = str(url, 'UTF8').strip()
                for char in r'><^\\{}|"':
                    if char in url:
                        continue
                while '&amp;' in url:
                    url = url.replace('&amp;', '&')
                if '#' in url:
                    url = url.split('#', 1)[0]
                if len(url) == 0:
                    continue
                url = re.search(r'^([^\s]+)', url).group(1)
                yield urllib.parse.quote(url, "!$&'()*+,/:;=?@[]-._~")


def extract(parenturl, d):
    """Extracts URLs from data.

    Args:
        parenturl (str): The parent URL.
        d (str): The candidate URL to be processed.

    Yields:
        str: The discovered URL.
    """
    for r in re.findall(b'([^"]+)', d):
        yield process_find(parenturl, r)
    for r in re.findall(b"([^']+)", d):
        yield process_find(parenturl, r)
    for r in re.findall(br'>\s*([^<\s]+)', d):
        yield process_find(parenturl, r)
    for r in re.findall(b"[^-]href='([^']+)'", d, re.I):
        yield process_find_href(parenturl, r)
    for r in re.findall(b'[^-]href="([^"]+)"', d, re.I):
        yield process_find_href(parenturl, r)
    for r in re.findall(br':\s*url\s*\(([^\)]+)\)', d, re.I):
        yield r


@functools.lru_cache()
def process_find_href(parenturl, d):
    """Processes a discovered possible URL from a ``href`` HTML attribute.

    Args:
        parenturl (str): The parent URL.
        d (str): The candidate URL to be processed.

    Returns:
        str: The processed URL. Returns None if not the candidate URL was found
            to be not a URL.
    """
    if d.startswith(b'?'):
        return re.search(br'^(https?://[^\?]+)', parenturl).group(1) + d
    if not re.search(br'^https?:\\?/\\?/', d):
        return process_find(parenturl, re.search(b'^(https?://.+/)',
                                                 parenturl).group(1))


@functools.lru_cache()
def process_find(parenturl, d):
    """Processes a discovered possible URL.

    Args:
        parenturl (str): The parent URL.
        d (str): The candidate URL to be processed.

    Returns:
        str: The processed URL. Returns None if not the candidate URL was found
            to be not a URL.
    """
    if re.search(b'^https?:////', d):
        return d.replace(b':////', b'://')
    if re.search(b'^https?://[^/]', d):
        return d
    if re.search(br'^https?:\\/\\?/', d):
        return d.replace(br'\\', b'')
    if d.startswith(br'\\/\\/'):
        return re.search(b'^(https?:)', parenturl).group(1) \
            + d.replace(br'\\', b'')
    if d.startswith(b'//'):
        return re.search(b'^(https?:)', parenturl).group(1) + d
    if d.startswith(br'\\/'):
        return re.search(b'^(https?://[^/]+)', parenturl).group(1) \
            + d.replace(br'\\', b'')
    if d.startswith(b'/'):
        return re.search(b'^(https?://[^/]+)', parenturl).group(1) + d

