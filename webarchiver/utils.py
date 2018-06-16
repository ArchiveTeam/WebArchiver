"""Functions for miscellaneous tasks."""
import hashlib
import math
import os
import random
import re
import string
import time


def strip_url_scheme(url):
    """Strips the scheme from an URL.

    Args:
        url (str): The URL to strip the scheme from.

    Returns:
        str: The URL with stripped scheme.
    """
    return re.sub('^https?://', '', url)


def sha512(s):
    """Calculates the SHA-512 hash of a string.

    Args:
        s (str or bytes): The data to calculate the SHA-512 hash of.

    Returns:
        str: The calculated SHA-512 hash.
    """
    if type(s) is str:
        s = s.encode('UTF-8')
    return hashlib.sha512(s).hexdigest()


def sha512_file(path):
    """Calculates the SHA-512 hash of the contents of a file.

    Args:
        path (str): The path to the file.

    Returns:
        str: The calculate SHA-512 hash.
    """ #TODO do not load full file in memory.
    with open(path, 'rb') as f:
        return sha512(f.read())


def sample(l, n):
    """Gets a number of random samples from list l.

    Args:
        l (set or list or dict): The list to pick from.
        n (int): The number of random samples.

    Returns:
        list: The list of random samples.
    """
    if type(l) is dict:
        l = set(l)
    if n == 0:
        return []
    if len(l) > n:
        return random.sample(l, n)
    return list(l)


def shuffle(l):
    """Gets a random item from a list.

    Args:
        l (set or list or dict): The list to pick from.

    Returns:
        The random item from the list.
    """
    return sample(l, len(l))


def check_time(old_time, max_diff_time):
    """Checks if a certain number of second have passed.

    The input time is checked for being at least a certain number of seconds in
    the past. The given time is checked with the current time.

    Args:
        old_time (int): The time to check.
        max_diff_time (int): The maximum number of passed seconds.

    Returns:
        bool: True if at least ``max_diff_time`` seconds have passed, else
            False.
    """
    return time.time() - old_time > max_diff_time


def split_set(l, n):
    """Splits a list into a number of sets.

    A given list is split up into smaller sets of items. If less items are in
    the list compared to the number of sets it should be split up into, some
    empty sets are created to create up to a number of sets.

    Args:
        l (set or list or dict): The list of items to split up.
        n (int): The number of sets to split the given list up in.

    Returns:
        list of sets: The list of created sets.
    """
    if type(l) is dict:
        l = set(l.values())
    elif type(l) is list:
        l = set(l)
    size = math.ceil(len(l)/n)
    lists = []
    if size == 0:
        size = 1
    for i in range(n):
        lists.append(set(sample(l, size)))
        l.difference_update(lists[-1])
    lists.extend([set()]*(n-len(lists)))
    return lists


def random_string(n):
    """Creates a random string consisting of letters and digits.

    Args:
        n (int): The length of the random string.

    Returns:
        str: A random string of a certain length consisting of characters
            matching [a-zA-Z0-9].
    """
    chars = string.ascii_lowercase + string.digits
    return ''.join(sample(chars, n))


def write_file(path, data, mode='wb'):
    """Safely write a file.

    The file is initially written with ``.`` and a random string of length 8
    appended. This name is changed to the normal name after the file is fully
    written. If the directory of the path of the file does not exist, it will
    be created.

    Args:
        path (str): The path to the file to write.
        data (str or bytes): The data to write to the file.
        mode (str, optional): The mode to open the file with. This should be a
            writable mode. Default is ``wb``.
    """
    temp = '{}.{}'.format(path, random_string(8))
    if not os.path.isdir(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    with open(temp, mode) as f:
        f.write(data)
    os.rename(temp, path)
    return True


def key_lowest_value(d):
    """Returns the key in a dict with the lowest value.

    Args:
        d (dict): The dict to use.

    Returns:
        The item in the dict with the lowest value.
    """
    return min(d, key=d.get)

