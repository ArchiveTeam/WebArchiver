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

Each stager can get a new job. The job can be issued to it by creating a file in directory `new-jobs`. If the file has extension `.ready` it is picked up by the stager and started.

### Crawling

The crawler received URLs from the stager it is connected to, crawls these URLs and send back the WARC and new found URLs.

## The protocol

WebArchiver communicates over TCP using its own protocol. The protocol is explained here.

The protocol has methods implemented. Data that comes with it is encoded in base64 and joined with a semicolon. On the receiving side data is split on the semicolon, decoded and processed.

Every piece of data is prepended with length. This length is an unsigned long. The length of the data is used to determine when all data is transferred.

