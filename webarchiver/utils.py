import hashlib
import math
import os
import random
import re
import string
import time


def random_string(l):
    return ''.join(random.choice(string.ascii_letters + string.digits, l))


def strip_url_scheme(url):
    return re.sub('^https?://', '', url)


def sha512(s):
    if type(s) is str:
        s = s.encode('UTF-8')
    return hashlib.sha512(s).hexdigest()


def sha512_file(filename):
    with open(filename, 'rb') as f:
        return sha512(f.read())


def sample(l, n):
    """Get a number of random samples from list l.

    Args:
        l: The list to pick from.
        n: The number of random samples.

    Returns:
        The list of random samples.
    """
    if type(l) is dict:
        l = set(l)
    if n == 0:
        return []
    if len(l) > n:
        return random.sample(l, n)
    return list(l)


def shuffle(l):
    return sample(l, len(l))


def check_time(old_time, max_time):
    return time.time() - old_time > max_time


def split_set(l, n):
    l = set(l.values())
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
    chars = string.ascii_lowercase + string.digits
    return ''.join(sample(chars, n))


def write_file(path, data, mode='wb'):
    temp = '{}.{}'.format(path, random_string(8))
    if not os.path.isdir(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    with open(temp, mode) as f:
        f.write(data)
    os.rename(temp, path)
    return True


def key_lowest_value(d):
    return min(d, key=d.get)

