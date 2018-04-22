# Archiver

**Archiver** is a decentralized web archiving system. It allows for servers to be added and removed and minimizes data-loss when a server is offline.

This project is still being developed.

## Usage

To run Archiver:
1. `git clone` this repository.
2. `cd` into it.
3. Run `python3 main.py` with options.

### Options

The following options are available for setting up a server in a network or creating a network.
* `-S SORT`
`--sort=SORT`: set the sort of server to be created. `SORT` can be `staging` for a staging server or `crawling` for a crawling server.
* `-H HOST`
`--host=HOST`: set the host of the staging server to connect to. `HOST` should can be the host or an IP. Should not be set if a network is being created.
* `-P PORT`
`--port=PORT`: set the port of the staging server to connect to. `PORT` should be a number. Should not be set if a network is being created.

## Servers

Archiver consists of staging and crawling servers. Staging servers divide the work among crawlers and other staging servers.

Settings for a server can be set by running
```bash
python3 settings.py [SETTINGS]
```
if no `[SETTINGS]` are given the script will ask for the change in settings. New settings are checked before being passed to the server. Be aware, the effects of a new setting cannot be undone.

### Staging server

The staging server distributes new jobs and URLs and received WARCs from crawlers.

Each staging server can get a new job. The job can be issued to it by creating a file in directory `new-jobs`. If the file has extension `.ready` it is picked up by the staging server and started.

### Crawling server

The crawling server received URLs from the staging server it is connected to, crawls these URLs and send back the WARC and new found URLs.

## The protocol

**Archiver** communicates over TCP using its own protocol. The protocol is explained here.

The protocol has methods implemented. Data that comes with it is encoded in base64 and joined with a semicolon. On the receiving side data is split on the semicolon, decoded and processed.

Every piece of data is prepended with length. This length is an unsigned long. The length of the data is used to determine when all data is transferred.

