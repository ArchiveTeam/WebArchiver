"""Tests for database.py."""
import os
import unittest

from webarchiver.database import UrlDeduplicationDatabase
from webarchiver.url import UrlConfig


class TestUrlDeduplicationDatabase(unittest.TestCase):
    """Tests for the database."""

    def test_single_url_exists(self):
        d = UrlDeduplicationDatabase('test', 'test')
        d.insert(UrlConfig('', 'https://example.org/', 0, ''))
        self.assertTrue(d.has_url('https://example.org/'))
        d.stop()
        d.clean()

    def test_single_url_not_exists(self):
        d = UrlDeduplicationDatabase('test', 'test')
        d.insert(UrlConfig('', 'https://example.org/', 0, ''))
        self.assertFalse(d.has_url('https://example.com/'))
        d.stop()
        d.clean()

    def test_multiple_urls_exists(self):
        d = UrlDeduplicationDatabase('test', 'test')
        d.insert(UrlConfig('', 'https://example.org/', 0, ''))
        d.insert(UrlConfig('', 'https://example.com/', 0, ''))
        d.insert(UrlConfig('', 'https://example.sometld/', 0, ''))
        self.assertTrue(d.has_url('https://example.org/'))
        self.assertTrue(d.has_url('https://example.com/'))
        self.assertTrue(d.has_url('https://example.sometld/'))
        d.stop()
        d.clean()


#class TestPayloadDeduplicationDatabase(unittest.TestCase):
#    def test_record(self):
#        pass #TODO

