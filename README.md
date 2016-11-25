dCache HPSS Provider
==============================================

This [dCache] plugin interfaces with HPSS. It expects the HPSS name space mounted
locally for data transfer. Additionally pre-staging is off-loaded to a [TReqS2] server
instances.

## Installation

To compile the plugin, run:

    mvn package

To install the plugin, unpack the resulting tarball in the dCache
plugin directory (usually `/usr/local/share/dcache/plugins`).

## Configuration

To use, define a nearline storage in the dCache admin interface:

    hsm create osm <the-hsm-name> Dc2Hpss <options>

Supported options:
* `-mountpoint`: Where the HPSS name space is mounted locally.
* `-treqs:server` : The server address of TReqS.
* `-treqs:port` : The port for contacting TReqS (default=7676).
* `-copies`: The number of parallel write and read transfers for the FUSE mount (default=5).

[dCache]: http://www.dcache.org/
[TReqs2]: https://gitlab.in2p3.fr/cc-in2p3-dev/treqs2
