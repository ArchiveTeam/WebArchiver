"""WARC processing."""
import hashlib
import logging
import os
import time
import urllib

from webarchiver.config import *
from webarchiver.request import get
from webarchiver.utils import strip_url_scheme, sha512

import warcio
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter

logger = logging.getLogger(__name__)


class WarcFile:
    """Class to load and process a WARC file.

    Attributes:
        directory_name (str): The directory the WARC file can be found in.
        filename (str): The filename of the WARC file.
        filename_deduplicated (str): The filename of the WARC that is created
            after deduplication.
        pathname (str): The path of the WARC file.
        pathname_deduplicated (str): The path of the WARC that is created after
            deduplication.
    """

    def __init__(self, directory_name):
        """Inits the :class:`WarcFile` object.

        Initializes the filenames and directory names of the current and to be
        created files. The WARC file should have the same name as the directory
        it is in, but with ``.warc.gz`` appended.

        Args:
            directory_name (str): The directory the WARC file can be found.

        Raises:
            FileNotFoundError: When the WARC file is not found.
        """
        self.directory_name = directory_name
        self.filename = str(int(time.time())) + '_' + \
                        os.path.basename(self.directory_name) + '.warc.gz'
        self.filename_deduplicated = self.filename.split('.')[0] + \
                                     '-deduplicated.warc.gz'
        self.pathname = os.path.join(self.directory_name, self.filename)
        self.pathname_deduplicated = os.path.join(self.directory_name,
                                                  self.filename_deduplicated)
        logger.debug('Prepare WARC file %s',
                     self.pathname, self.pathname_deduplicated)

    def deduplicate(self):
        """Deduplicates the WARC file.

        Each response WARC record from the original WARC file is checked for
        being a duplicate with function :func:`self._record_is_duplicate`. If a
        duplicate is found, the record is converted to a revisit record and
        written to the deduplicated WARC file. If no deduplicate is found the
        original record is written to the deduplicated WARC file.
        """
        if not os.path.isfile(self.pathname):
            logger.error('WARC file %s not found.', self.pathname)
            raise FileNotFoundError(self.pathname)
        logger.info('Deduplicating WARC file %s into WARC file %s.',
                    self.pathname, self.pathname_deduplicated)
        with open(self.pathname, 'rb') as f_in, \
                open(self.pathname_deduplicated, 'wb') as f_out:
            writer = WARCWriter(filebuf=f_out, gzip=True)
            for record in ArchiveIterator(f_in):
                if record.rec_headers.get_header('WARC-Type') == 'response':
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    digest = record.rec_headers \
                                   .get_header('WARC-Payload-Digest')
                    logger.debug('Deduplicating record %s %s.', url, digest)
                    duplicate = self._record_is_duplicate(url, digest)
                    if not duplicate:
                        logger.debug('Record %s %s is not a duplicate.', url,
                                     digest)
                        writer.write_record(record)
                    else:
                        logger.debug('Record %s %s is a duplicate.', url,
                                     digest)
                        writer.write_record(
                            self._record_response_to_revisit(writer, record,
                                                             duplicate)
                        )
                else:
                    writer.write_record(record)

    def _record_is_duplicate(self, url, digest):
        """Checks if a record is a duplicate.

        The SHA-512 hash of a combination of the digest and the URL stripped
        from its scheme is checked against previous hashes. If a duplicate is
        found the data from the other record will be returned.

        Args:
            url (str): The URL of the WARC record to be deduplicated.
            digest (str): The digest of the payload of the record to be
                deduplicated. This should be a SHA-1 digest and the string
                should start with ``sha1:``.

        Returns:
            list: The data of WARC record that is being deduplicated against.
            bool: False if the record is not found to be a duplicate record.
        """ #TODO fix deduplication server #TODO add info on returned data
        assert digest.startswith('sha1:')
        logger.debug('Checking if record %s %s is a duplicate.', url, digest)
        digest = digest.split(':', 1)[1]
        hashed = sha512('{};{}'.format(digest, strip_url_scheme(url)))
        response = get(urllib.parse.urljoin(DEDUPLICATION_SERVER,
                                            hashed))
        if not response or ';' not in response.text:
            return False
        return response.text.split(';', 1)

    def _record_response_to_revisit(self, writer, record, duplicate):
        """Converts a resonse WARC record to a revisit WARC record.

        The ``WARC-Refers-To-Date`` header is set to the date the revisit
        record points to and the ``WARC-Refers-To-Target-URI`` header to its
        URL. The header ``WARC-Type`` is set with value ``revisit``,
        ``WARC-Truncated`` with value length and ``WARC-Profile`` with value
        ``http://netpreserve.org/warc/1.0/revisit/identical-payload-digest``.
        The values for the headers ``WARC-Block-Digest`` and ``Content-Length``
        are recalculated.

        Args:
            writer (:obj:`warcio.warcwriter.WARCWriter`): The writer for the
                deduplicated WARC file.
            record (:obj:`warcio.recordloader.ArcWarcRecord`): The record that
                should be converted to a revisit record.
            duplicate (list): The data of the record that is being deduplicated
                against.

        Returns:
            :obj:`warcio.recordloader.ArcWarcRecord`: The revisit record.
        """ #TODO better logging. all header changes?
        warc_headers = record.rec_headers
        warc_headers.replace_header('WARC-Refers-To-Date',
                                    datetime.strptime(duplicate,
                                                      '%Y%m%d%H%M%S') \
                                            .strftime('%Y-%m-%DT%H:%M:%SZ'))
        warc_headers.replace_header('WARC-Refers-To-Target-URI', duplicate[1])
        warc_headers.replace_header('WARC-Type', 'revisit')
        warc_headers.replace_header('WARC-Truncated', 'length')
        warc_headers.replace_header('WARC-Profile',
                                    'http://netpreserve.org/warc/1.0/' \
                                    'revisit/identical-payload-digest')
        warc_headers.remove_header('WARC-Block-Digest')
        warc_headers.remove_header('Content-Length')
        return writer.create_warc_record(record.rec_headers \
                                               .get_header('WARC-Target-URI'),
                                         'revisit', warc_headers=warc_headers,
                                         http_header=record.http_headers)

