"""Databases used in webarchiver."""
import logging
import os
import sqlite3

logger = logging.getLogger(__name__)


class BaseDatabase:
    """The base class for the database."""

    def __init__(self, path, synchronous, journal_mode):
        """Inits the database.

        The database is created at the path with ``.db`` appended to it if not
        yet appended. Synchronous mode can be ``OFF`` or ``ON``. Journal mode
        can be ``OFF``, ``WAL`` or ``MEMORY``. See
        `SQLite PRAGMA statements <https://www.sqlite.org/pragma.html>`_ for
        information about these options.
        """
        assert synchronous in ('OFF', 'ON')
        assert journal_mode in ('OFF', 'WAL', 'MEMORY')
        self._path = '{}.db'.format(path)
        logger.debug('Database %s; connecting.', path)
        self._con = sqlite3.connect(self._path)
        logger.debug('Database %s; getting cursor.', path)
        self._cur = self._con.cursor()
        logger.debug('Database %s; synchronous=%s.', path, synchronous)
        logger.debug('Database %s; journal mode=%s.', path, journal_mode)
        self._cur.execute('PRAGMA synchronous=%s'.format(synchronous))
        self._cur.execute('PRAGMA journal_mode=%s'.format(journal_mode))

    def insert(self):
        pass

    def stop(self):
        """Stops the running database.

        The database cursor is closed, changed are committed and the database
        file is closed.
        """
        logger.debug('Database %s; closing cursor.', self._path)
        self._cur.close()
        logger.debug('Database %s; committing.', self._path)
        self._con.commit()
        logger.debug('Database %s; closing connection.', self._path)
        self._con.close()

    def clean(self):
        """Removes the database file."""
        logger.debug('Database %s; closing cursor.', self._path)
        os.remove(self._path)


class UrlDeduplicationDatabase(BaseDatabase):
    """The database for URL deduplication."""

    def __init__(self, path, name):
        """Inits the database.

        Uses ``OFF`` for synchronous and ``WAL`` for journal mode. A table is
        used with values::

            (url TEXT, depth INTEGER, parent TEXT)

        Args:
            path (str): The path of the database file.
            name (str): The name of the table in the database.
        """
        super().__init__(path, 'OFF', 'WAL')
        self._name = name
        logger.debug('Database %s; table %s; creating.', self._path,
                     self._name)
        self._cur.execute('CREATE TABLE {} ' \
                          '(url TEXT, depth INTEGER, parent TEXT)' \
                          .format(self._name))

    def insert(self, urlconfig):
        """Insert an URL into the database.

        The data for URL, depth and parent URL from the
        :class:`webarchiver.url.UrlConfig` object is added into the database.

        Args:
            urlconfig (:obj:`webarchiver.url.UrlConfig`): The configuration for
                the URL to be added.
        """# TODO assertions
        logger.debug('Database %s; table %s; adding %s.', self._path,
                     self._name, urlconfig)
        self._cur.execute('INSERT INTO {} VALUES (?,?,?)'.format(self._name), 
                          (urlconfig.url, urlconfig.depth,
                           urlconfig.parent_url \
                           if urlconfig.parent_url is not None else ''))

    def has_url(self, url):
        """Checks if the database holds an URL.

        Note:
            The other set values for the URL in the database are not matched.

        Args:
            url (str): The URL to check the database for.

        Returns:
            bool: True if the URL is in the database, else False.
        """
        logger.debug('Database %s; table %s; checking URL %s.', self._path,
                     self._name, url)
        self._cur.execute('SELECT 1 FROM {} WHERE url=? LIMIT 1'
                          .format(self._name), (url,))
        return self._cur.fetchone() is not None


#class PayloadDeduplicationDatabase(BaseDatabase):
#    def __init__(self, path, name):
#        super().__init__(path, 'OFF', 'WAL')
#        self._name = name
#        self._cur.execute('CREATE TABLE {} (url TEXT, payload TEXT)'
#                          .format(self._name))
#
#    def insert(self, url, payload):
#        # TODO assertions
#        self._cur.execute('INSERT INTO {} VALUES (?, ?)' .format(self._name),
#                          (url, payload))

__all__ = ('UrlDeduplicationDatabase',)

