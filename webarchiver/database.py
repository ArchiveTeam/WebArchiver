import os
import sqlite3


class BaseDatabase:
    def __init__(self, path, synchronous, journal_mode):
        assert synchronous in ('OFF', 'ON')
        assert journal_mode in ('OFF', 'WAL', 'MEMORY')

        self._path = '{}.db'.format(path)
        self._con = sqlite3.connect(self._path)
        self._cur = self._con.cursor()
        self._cur.execute('PRAGMA synchronous={}'.format(synchronous))
        self._cur.execute('PRAGMA journal_mode={}'.format(journal_mode))

    def insert(self):
        pass

    def stop(self):
        self._cur.close()
        self._con.commit()
        self._con.close()

    def clean(self):
        os.remove(self._path)


class UrlDeduplicationDatabase(BaseDatabase):
    def __init__(self, path, name):
        super().__init__(path, 'OFF', 'WAL')
        self._name = name
        self._cur.execute('CREATE TABLE {} ' \
                          '(url TEXT, depth INTEGER, parent TEXT)' \
                          .format(self._name))

    def insert(self, urlconfig):
        # TODO assertions
        self._cur.execute('INSERT INTO {} VALUES (?,?,?)'.format(self._name), 
                          (urlconfig.url, urlconfig.depth,
                           urlconfig.parent_url \
                           if urlconfig.parent_url is not None else ''))

    def has_url(self, url):
        self._cur.execute('SELECT 1 FROM {} WHERE url=? LIMIT 1'
                          .format(self._name), (url,))
        return self._cur.fetchone() is not None


class PayloadDeduplicationDatabase(BaseDatabase):
    def __init__(self, path, name):
        super().__init__(path, 'OFF', 'WAL')
        self._name = name
        self._cur.execute('CREATE TABLE {} (url TEXT, payload TEXT)'
                          .format(self._name))

    def insert(self, url, payload):
        # TODO assetions
        self._cur.execute('INSERT INTO {} VALUES (?, ?)' .format(self._name),
                          (url, payload))

__all__ = (UrlDeduplicationDatabase, PayloadDeduplicationDatabase)

