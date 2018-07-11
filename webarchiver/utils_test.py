"""Tests for utils.py."""
import unittest

from webarchiver.utils import *


class TestStripUrlScheme(unittest.TestCase):
    """Tests for stripping URL schemes."""

    def test_http(self):
        self.assertEqual(strip_url_scheme('http://example.com/'),
                         'example.com/')

    def test_https(self):
        self.assertEqual(strip_url_scheme('https://example.com/'),
                         'example.com/')


class TestSha512(unittest.TestCase):
    """Tests for the calculation of a SHA512 hash."""

    def test_str(self):
        self.assertEqual(sha512('teststr'), '037e05dbd5cc19d7aac5fee7205d090' \
                         '5e454ac67cb62d801d7241db091bfe7823218d628ad192419f' \
                         '4956afac48039bd9ae770410da6c837b003482bc528505b')

    def test_bytes(self):
        self.assertEqual(sha512(b'testbytes'), '2312b40ce2436b879c5c8e894ad1' \
                         'e86adb31cb5018787233d7065ce71471185284126044669ad9' \
                         '4bd22dd95b3e63e7ca55b0d7a73669d50af37cc07f204db89d')


class TestSample(unittest.TestCase):
    """"Tests for selecting a sample from a list, set or dict."""

    def test_type(self):
        self.assertIs(type(sample([1, 2, 3, 4], 2)), list)
        self.assertIs(type(sample({1, 2, 3, 4}, 2)), list)
        self.assertIs(type(sample({1: 'a', 2: 'b', 3: 'c', 4: 'd'}, 2)), list)

    def test_list(self):
        self.assertEqual(len(sorted(sample([1, 2, 3, 4], 0))), 0)
        self.assertEqual(len(sorted(sample([1, 2, 3, 4], 2))), 2)
        self.assertEqual(len(sorted(sample([1, 2, 3, 4], 6))), 4)

    def test_set(self):
        self.assertEqual(len(sorted(sample({1, 2, 3, 4}, 0))), 0)
        self.assertEqual(len(sorted(sample({1, 2, 3, 4}, 2))), 2)
        self.assertEqual(len(sorted(sample({1, 2, 3, 4}, 6))), 4)

    def test_dict(self):
        self.assertEqual(len(sorted(sample({1: 'a', 2: 'b', 3: 'c', 4: 'd'},
                                           0))), 0)
        self.assertEqual(len(sorted(sample({1: 'a', 2: 'b', 3: 'c', 4: 'd'},
                                           2))), 2)
        self.assertEqual(len(sorted(sample({1: 'a', 2: 'b', 3: 'c', 4: 'd'},
                                           6))), 4)


class TestShuffle(unittest.TestCase):
    """Tests for shuffling a list, set or dict."""

    def test_basic(self):
        self.assertListEqual(shuffle([]), [])
        self.assertEqual(len(sorted(shuffle([1, 2, 3, 4]))), 4)


class TestSplitSet(unittest.TestCase):
    """Tests for splitting a list, set or dict."""

    def test_basic(self):
        test_set = {1, 2, 3, 4, 5, 6}
        result = split_set(test_set, 3)
        self.assertIs(type(result), list)
        self.assertEqual(len(result), 3)
        for r in result:
            self.assertIs(type(r), set)
            self.assertEqual(len(r), 2)
        self.assertSetEqual({s for r in result for s in r}, test_set)

    def test_too_small(self):
        test_set = {1, 2, 3, 4}
        result = split_set(test_set, 7)
        self.assertEqual(len(result), 7)
        for r in result[4:]:
            self.assertEqual(len(r), 0)
        self.assertSetEqual({s for r in result for s in r}, test_set)

    def test_special_length(self):
        test_set = {1, 2, 3}
        result = split_set(test_set, 2)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 1)
        self.assertSetEqual({s for r in result for s in r}, test_set)


class TestRandomString(unittest.TestCase):
    """Tests for creating a random string."""

    def test_length(self):
        result = random_string(20)
        self.assertEqual(len(result), 20)
        chars = string.ascii_lowercase + string.digits
        for c in result:
            self.assertIn(c, chars)


class TestWriteFile(unittest.TestCase):
    """"""
    pass


class TestKeyLowestValue(unittest.TestCase):
    """"""
    pass

