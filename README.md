# dCache HPSS Provider

This [dCache] plugin interfaces with HPSS. It expects the HPSS name space
mounted locally for data transfer. Additionally pre-staging is off-loaded
to a [TReqS2] server instances.

## Installation

To compile the plugin, run:

    mvn package

To install the plugin, unpack the resulting tarball in the dCache
plugin directory (usually `/usr/local/share/dcache/plugins`).

## Configuration

To use, define a nearline storage in the dCache admin interface:

    hsm create osm <the-hsm-name> Dc2Hpss <options>

Supported options:
* `-mountpoint`    : *Mandatory!* Where the HPSS name space is mounted
                     locally.
* `-treqsHost`     : *Mandatory!* The server address of TReqS.
* `-treqsPort`     : The port for contacting TReqS (default=8080).
* `-treqsUser`     : The user login for working with TReqS (default=treqs).
* `-treqsPassword` : The password that belongs to the user (default=changeit).
* `-copies`        : The number of parallel write and read transfers for the
                     FUSE mount (default=5).
* `-hpssRoot`      : When recalling data from HPSS, TReqS needs to submit the
                     full logical path for the file in the HPSS namespace.
                     Because dCache uses a local FUSE mount for data transfer,
                     the full logical path is unknown without this setting.
                     By default, '/' is assumed.
* `-period`        : dCache will query TReqS for the state of current requests
                     every x seconds (by default x=120).

[dCache]: http://www.dcache.org/
[TReqs2]: https://gitlab.in2p3.fr/cc-in2p3-dev/treqs2
