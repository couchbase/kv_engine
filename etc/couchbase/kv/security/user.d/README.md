# User rate limits

This directory contains description of the user rate limits. When a client
connects for the first time it gets assigned to the limits specified in
`default.json`. The system will then try to look for a configuration file
named `<username>.json` and if such a file exists the constraints in that
file will be applied.

The format of these files is JSON and looks like:

    {
      "connections": 1,
      "egress": 1024,
      "ingress": 1024,
      "operations": 10
    }

* `connections` Is the total number of concurrent client connection the user
  may connect to the node.
* `egress` Is the total number of bytes the client may request from the server
  per minute. Error messages related to packet validation, access control
  or rate limitations will override the limit
* `ingress` is the total number of bytes the client may send to the server
  per minute. Given that we can't stop the client from sending the data to
  the server will read the request but return a rate limited error message
  instead of executing the packet
* `operations` is the total number of operations the client may execute per
  minute.