WebArchiver
====

**WebArchiver** is a decentralized web archiving system. It allows for servers to be added and removed and minimizes data-loss when a server is offline.

This project is still being developed.

Usage
----

WebArchiver has the following dependencies:

 * ``flask``
 * ``requests``
 * ``warcio``

Install these by running ``pip install flask requests warcio`` or use ``pip3`` in case your default Python version is Python 2.

``wget`` is also required, this can be installed using::

    sudo apt-get install wget

To run WebArchiver:
 #. ``git clone`` this repository,
 #. ``cd`` into it,
 #. Run ``python main.py`` with options or use ``python3`` is your default Python version is Python 2.

Options
~~~~

The following options are available for setting up a server in a network or creating a network.

 * ``-h``

   ``--help``
 * ``-v``

   ``-version``: Get the version of WebArchiver.
 * ``-S SORT``

   ``--sort=SORT``: The sort of server to be created. ``SORT`` can be ``stager`` for a stager or ``crawler`` for a crawler. This argument is required.
 * ``-SH HOST``

   ``--stager-host=HOST``: The host of the stager to connect to. This should not be set if this is the first stager.
 * ``-SP PORT``

   ``--stager-port=PORT``: The port of the stager to connect to. This should not be set if this is the first stager.
 * ``-H HOST``

   ``--host=HOST``: The host to use for communication. If not set the scripts will try to determine the host.
 * ``-P PORT``

   ``--port=PORT``: The port to use for communication. If not set a random port between 3000 and 6000 will be chosen.
 * ``--no-dashboard``: Do not create a dashboard.
 * ``--dashboard-port``: The port to use for the dashboard.

Add a job
~~~~

A crawl of a website or a list of URLs is called a job. To add a job a configuration file needs to be processed and added to WebArchiver. The configuration file has the identifier and the following possible options.

 * ``url``: URL to crawl.
 * ``urls file``: Filename of a file containing a list of URLs.
 * ``urls url``: URL to a webpage containing a raw list of URLs.
 * ``rate``: URL crawl rate in URLs per second.
 * ``allow regex``: Regular expression a discovered URL should match.
 * ``ignore regex``: Regular expression a discovered URL should not match.
 * ``depth``: Maximum depth to crawl.

For all settings except ``rate`` and ``depth`` multiple entries are possible.

An example of a configuration file is

.. code:: ini

    [identifier]
    url = https://example.com/
    url = https://example.com/page2
    urls file = list
    urls url = https://pastebin.com/raw/tMpQQk7B
    rate = 4
    allow regex = https?://(?:www)?example\.com/
    allow regex = https?://[^/]+\.london
    ignore regex = https?://[^/]+\.nl
    depth = 3

To process the configuration file and add it to WebArchiver, run ``python add_job.py FILENAME``, where ``FILENAME`` is the name of the configuration file.

Servers
----

WebArchiver consists of stagers and crawlers. Stagers divide the work among crawlers and other stagers.

Stager
~~~~

The stager distributes new jobs and URLs and received WARCs from crawlers.

Crawling
~~~~

The crawler received URLs from the stager it is connected to, crawls these URLs and send back the WARC and new found URLs.

