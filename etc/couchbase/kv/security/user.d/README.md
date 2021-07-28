# User rate limits

This directory contains description of the user rate limits. When a client
connects for the first time it gets assigned to the limits specified in
`default.json`. The system will then try to look for a configuration file
named `<username>.json` and if such a file exists the constraints in that
file will be applied.

The format of these files is JSON and looks like:

    {
      "egress_mib_per_min": 1024,
      "ingress_mib_per_min": 1024,
      "num_connections": 1,
      "num_ops_per_min": 10
    }

* `num_connections` Is the total number of concurrent client connection the user
  may connect to the node.
* `egress_mib_per_min` Is the number of million bytes the client may request
  from the server per minute. Error messages related to packet validation,
  access control or rate limitations will override the limit
* `ingress_mib_per_min` is the number of million bytes the client may send
  to the server per minute. Given that we can't stop the client from sending
  the data to the server will read the request but return a rate limited error
  message instead of executing the packet
* `num_ops_per_min` is the total number of operations the client may execute per
  minute.