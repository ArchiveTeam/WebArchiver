import time

import requests


def get(url, status_codes=[200], content_length=0, max_tries=1,
        headers=None, cookies=None, preserve_url=False, stream=False,
        sleep_time=5, session=None):
    tries = 0
    while tries < max_tries:
        try:
            response = (session or requests).get(url, headers=headers,
                                                 cookies=cookies,
                                                 stream=stream)
            if not stream:
                assert len(response.text) > content_length
            assert response.status_code in status_codes
            if preserve_url:
                assert response.url == url
            return response
        except:
            tries += 1
            if tries < max_tries:
                time.sleep(sleep_time)
    return False

