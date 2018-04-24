import os
import unittest

from webarchiver.database import UrlDeduplicationDatabase


class TestUrlDeduplicationDatabase(unittest.TestCase):
    def test_url_exists(self):
        d = UrlDeduplicationDatabase('test.db', 'test')
        d.insert('https://www.archiveteam.org/')
        self.assertTrue(d.has_url('https://www.archiveteam.org/'))
        self.assertFalse(d.has_url('https://tracker.archiveteam.org/'))
        d.stop()
        os.remove('test.db')


class TestPayloadDeduplicationDatabase(unittest.TestCase):
    def test_record(self):
        pass #TODO

