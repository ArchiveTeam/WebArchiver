# WebArchiver

**WebArchiver** is a decentralized web archiving system. It allows for servers to be added and removed and minimizes data-loss when a server is offline.

This project is still being developed.

## Usage

To run WebArchiver:
1. `git clone` this repository.
2. `cd` into it.
3. Run `python3 main.py` with options.

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

## Servers

WebArchiver consists of stagers and crawlers. Stagers divide the work among crawlers and other stagers.

### Stager

The stager distributes new jobs and URLs and received WARCs from crawlers.

### Crawling

The crawler received URLs from the stager it is connected to, crawls these URLs and send back the WARC and new found URLs.

## The protocol

TODO

