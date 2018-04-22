import hashlib
import os
import time
import urllib

from archiver.config import *
from archiver.request import get
from archiver.utils import strip_url_scheme, sha512

import warcio
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter


class WarcFile:
    def __init__(self, directory_name):
        self.directory_name = directory_name
        self.filename = str(int(time.time())) + '_' + \
                        os.path.basename(self.directory_name) + '.warc.gz'
        self.filename_deduplicated = self.filename.split('.')[0] + \
                                     '-deduplicated.warc.gz'
        self.pathname = os.path.join(self.directory_name, self.filename)
        self.pathname_deduplicated = os.path.join(self.directory_name,
                                                  self.filename_deduplicated)

    def deduplicate(self):
        assert os.path.isfile(self.pathname)
        with open(self.pathname, 'rb') as f_in, \
                open(self.pathname_deduplicated, 'wb') as f_out:
            writer = WARCWriter(filebuf=f_out, gzip=True)
            for record in ArchiveIterator(f_in):
                if record.rec_headers.get_header('WARC-Type') == 'response':
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    digest = record.rec_headers \
                                   .get_header('WARC-Payload-Digest')
                    duplicate = self._record_is_duplicate(url, digest)
                    if not duplicate:
                        writer.write_record(record)
                    else:
                        writer.write_record(
                            self._record_response_to_revisit(writer, record,
                                                             duplicate)
                        )
                else:
                    writer.write_record(record)

    def _record_is_duplicate(self, url, digest):
        assert digest.startswith('sha1:')
        digest = digest.split(':', 1)[1]
        hashed = sha512('{};{}'.format(digest, strip_url_scheme(url)))
        response = get(urllib.parse.urljoin(DEDUPLICATION_SERVER,
                                            hashed))
        if not response or ';' not in response.text:
            return False
        return response.text.split(';', 1)

    def _record_response_to_revisit(self, writer, record, duplicate):
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

