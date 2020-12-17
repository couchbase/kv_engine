# Portnumber file format

The file specified as `portnumber_file` in [memcached.json](memcached.json.adoc)
contains a JSON object with the following format:

    {
      "ports": [
        {
          "family": "inet",
          "host": "0.0.0.0",
          "port": 35273,
          "system": false,
          "tag": "plain",
          "tls": false,
          "type": "mcbp",
          "uuid": "c54e2ec2-277e-4531-87ad-3955cb4236c5"
        },
        {
          "family": "inet6",
          "host": "::",
          "port": 39369,
          "system": false,
          "tag": "plain",
          "tls": false,
          "type": "mcbp",
          "uuid": "cc480095-7e1c-4f24-f7c7-deb32f413076"
        },
        {
          "family": "inet",
          "host": "0.0.0.0",
          "port": 40385,
          "system": true,
          "tag": "TLS IPv4",
          "tls": true,
          "type": "mcbp",
          "uuid": "28ecaebb-8350-4bf6-f08f-58fadc6642a8"
        },
        {
          "family": "inet6",
          "host": "::",
          "port": 42441,
          "system": true,
          "tag": "TLS Ipv6",
          "tls": true,
          "type": "mcbp",
          "uuid": "5c3b9813-f58f-458e-f713-cf8db3aae18e"
        }
      ],
      "prometheus": {
        "family": "inet",
        "port": 43455
      }
    }

`ports` contains an array of all ports the server listen to which supports
the memcached binary protocol

`prometheus` contains an object describing the port providing support for
prometheus stats collection.

## Ports entry

* `family` may be `inet` (for IPv4) or `inet6`(for IPv6)
* `host` is the hostname the interface address port is bound to
* `port` is the port number it is bound to
* `system` if connections should be counted as system connections or user
* `tag` a free form name for the interface
* `tls` set to true if the port is configured to use TLS
* `type` should always be `mcbp` (memcached binary protocol)
* `uuid` a unique identifier for the port
