# WebArchiver

**WebArchiver** is a decentralized web archiving system. It allows for servers to be added and removed and minimizes data-loss when a server is offline.

This project is still being developed.

## Usage

WebArchiver has the following dependencies:
* `requests`
* `warcio`

Install these by running `pip install warcio requests` or use `pip3` in case your default Python version is Python 2.

To run WebArchiver:
1. `git clone` this repository.
2. `cd` into it.
3. Run `python main.py` with options or use `python3` is your default Python version is Python 2.

### Options

The following options are available for setting up a server in a network or creating a network.
* `-h`
  `--help`
* `-S SORT`
  `--sort=SORT`: The sort of server to be created. `SORT` can be `stager` for a stager or `crawler` for a crawler. This argument is required.
* `-SH HOST`
  `--stager-host=HOST`: The host of the stager to connect to. This should not be set if this is the first stager.
* `-SP PORT`
  `--stager-port=PORT`: The port of the stager to connect to. This should not be set if this is the first stager.
* `-H HOST`
  `--host=HOST`: The host to use for communication. If not set the scripts will try to determine the host.
* `-P PORT`
  `--port=PORT`: The port to use for communication. If not set a random port between 3000 and 6000 will be chosen.

### Add a job

A crawl of a website or a list of URLs is called a job. To add a job a configuration file needs to be processed and added to the network. The configuration file has the identifier and the following possible options.
* `url`: An URL to crawl.
* `urls file`: Filename of a file containing a list of URLs.
* `urls url`: URL to a webpage contaiing a raw list of URLs.
* `rate`: URL crawl rate in URL per second.
* `regex`: Regex a discovered URL should match.
* `depth`: Depth to crawl.

For all settings except `rate` and `depth` multiple entries are possible.

An example of a configuration file is
```ini
[identifier]
url = https://example.com/
url = https://example.com/page2
urls file = list
urls url = https://pastebin.com/raw/tMpQQk7B
rate = 4
regex = https?://(?:www)?example\.com/
regex = https?://[^/]+\.london
depth = 3
```

To process the configuration file and add it to WebArchiver, run `python add_job.py FILENAME`, where `FILENAME` if the name of the configuration file.

## Servers

WebArchiver consists of stagers and crawlers. Stagers divide the work among crawlers and other stagers.

### Stager

The stager distributes new jobs and URLs and received WARCs from crawlers.

### Crawling

The crawler received URLs from the stager it is connected to, crawls these URLs and send back the WARC and new found URLs.

