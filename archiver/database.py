import sqlite3


class BaseDatabase:
    def __init__(self, path, synchronous, journal_mode):
        assert synchronous in ('OFF', 'ON')
        assert journal_mode in ('OFF', 'WAL', 'MEMORY')

        self._con = sqlite3.connect(path)
        self._cur = self._con.cursor()
        self._cur.execute('PRAGMA synchronous={}'.format(synchronous))
        self._cur.execute('PRAGMA journal_mode={}'.format(journal_mode))

    def insert(self):
        pass

    def stop(self):
        self._cur.close()
        self._con.commit()
        self._con.close()


class UrlDeduplicationDatabase(BaseDatabase):
    def __init__(self, path, name):
        super().__init__(path, 'OFF', 'WAL')
        self._name = name
        self._cur.execute('CREATE TABLE {} (url TEXT)'.format(self._name))

    def insert(self, url):
        # TODO assertions
        self._cur.execute('INSERT INTO {} VALUES (?)'.format(self._name), 
                          (url,))

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

