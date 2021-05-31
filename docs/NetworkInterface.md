# Network interface handling in memcached

During startup memcached binds to ephemeral ports on the
loopback interface and writes the port(s) to the file specified in
[`portnumber_file`](PortumberFileFormat.md) in the provided configuration
(it is the responsibility of the process starting memcached to ensure that
the directory is writable for memcached).

Once the file exists memcached is "up'n'running" and listening
to the ports listed in the file and ready for clients to connect
and perform additional configuration.

Note that after a restart the configuration is gone and memcached
will try to bind to _new_ ephemeral ports and store them in the
`portnumber_file`.

A controlling process could implement the logic by doing something like:

    config["portnumber_file"] = "/opt/couchbase/var/lib/couchbase/run/memcached.ports.0001";
    write_file("/opt/couchbase/var/lib/couchbase/etc/memcached.json", config);
    auto handle = startProcess("/opt/couchbase/bin/memcached", "-C", "/opt/couchbase/var/lib/couchbase/etc/memcached.json");
    while (handle->isRunning() && !isFile(config["portnumber_file"])) {
       backoff();
    }
    ports = readJsonFile(config["portnumber_file"])["ports"];
    // memcached is up'n'running, set up TLS
    nlohmann::json tls_properties = {
        {"private key", "/opt/couchbase/var/lib/couchbase/etc/private.pem"},
        {"certificate chain", "/opt/couchbase/var/lib/couchbase/etc/certificate.cert"},
        {"CA file", "/opt/couchbase/var/lib/couchbase/etc/CAfile.pem"},
        {"password", cb::base64::encode("This is the passphrase")},
        {"minimum version", "tlsv1"},
        {"cipher list",
         {{"tls 1.2", "HIGH"},
          {"tls 1.3",
           "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_"
           "AES_"
           "128_GCM_SHA256:TLS_AES_128_CCM_8_SHA256:TLS_AES_128_CCM_"
           "SHA256"}}},
        {"cipher order", true},
        {"client cert auth", "disabled"}};
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
    cmd.setKey("tls");
    cmd.setValue(tls_properties.dump());
    auto rsp = conn.execute(cmd);
    if (!rsp.isSuccess()) {
      // handle error!
    }
    nlohmann::json descr = {{"host", "*"},
                            {"port", 11210},
                            {"family", "inet"},
                            {"system", false},
                            {"type", "mcbp"},
                            {"tls", true};
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
    cmd.setKey("define");
    cmd.setValue(descr.dump());
    auto rsp = conn.execute(cmd);
    if (!rsp.isSuccess()) {
       // handle error
    }
    ports.addInterface(rsp.getValue()):
    // people may now connect port 11210 using TLS

## ifconfig command

The ifconfig command takes a command name in the key field, and
the value field contains a command-specific payload.

The connection must hold the `NodeManagement` privilege to run
the command.

### Define port

Key - Define

The body contains a JSON document describing the interface to
create:

    {
      "host" : "hostname"
      "port" : port,
      "family" : "inet[6]",
      "tls" : true,
      "system" : false,
      "tag" : "optional free form name for the interface",
      "type": "interface type (see below)"
    }

* `host` may be set to `*` to bind to all interfaces, a textual
  name to look up or an ip address. (defaults to "localhost").

* `port` may be `0` (default) to let the kernel pick an ephemeral port

* `family` may be `inet` for IPv4 and `inet6` for IPv6 (mandatory).

* `tls` is a boolean value indicating if the port is set
  up to use OpenSSL or not (if set to true the TLS configuration must
  be specified for the port to "work" (otherwise the client will be
  disconnected). No attempts will be made to make sure that this
  interface is working when it is defined, as it is possible to
  reconfigure the system so that the interface will no longer
  work at a later time (for instance by providing obsolete
  certificates etc)).

* `system` indicates if connections over this interface should be counted for
  from the the system connection pool or the user connection pool.

* `type` may be one of the following:

    mcbp          The network interface should be used
                  by normal client-server traffic using
                  the memcached binary protocol

    prometheus    The network interface should be used
                  by prometheus to collect statistics.
                  host, ssl and system must not be set.

Upon success (at least one port created) the returned payload is a
JSON document with all of the properties for the newly created ports
and an array containing all errors returned while trying to create
the ports (for instance if one port failed to bind)

    {
      "errors": [
        "error message 1",
        "error message 2"
      ],
      "ports": [
        {
          "family": "inet",
          "host": "0.0.0.0",
          "port": 38649,
          "system": false,
          "tag": "yo",
          "type": "mcbp",
          "uuid": "ec3db10c-762c-4205-c4ce-b02c014fb44c"
        }
      ]
    }

If the status of the operation can't be represented with the
standard error codes (`einval`, `eaccess` etc) the returned
payload contains a JSON document with the following layout:

    {
      "error": {
        "context" : "Failed to create any ports",
        "errors": [
          "message 1",
          "message 2"
        ]
      }
    }

### Delete port

Key - Delete

The body contains a the uuid for the interface to delete.

If the status of the operation can't be represented with the
standard error codes (`success`, `enoent`, `einval`, `eaccess`
etc) the returned payload contains more information just like
create.

### List ports

Key - List

Upon success the following JSON payload is returned:

    [
      {
        "family": "inet",
        "host": "0.0.0.0",
        "port": 34961,
        "system": false,
        "tag": "plain",
        "type": "mcbp",
        "uuid": "f6f6a824-d702-4208-2e7e-106f8b6661e1"
      },
      {
        "family": "inet6",
        "host": "::",
        "port": 39445,
        "system": false,
        "tag": "plain",
        "type": "mcbp",
        "uuid": "a0f99fa0-df5a-4e64-e86e-59e242277758"
      },
      {
        "family": "inet",
        "host": "0.0.0.0",
        "port": 36437,
        "system": true,
        "tag": "ssl",
        "tls": true,
        "type": "mcbp",
        "uuid": "5d64edf3-6bf0-4fe5-583e-68ceff8e4d04"
      },
      {
        "family": "inet6",
        "host": "::",
        "port": 40855,
        "system": true,
        "tag": "ssl",
        "tls": true,
        "type": "mcbp",
        "uuid": "5b6c27db-a214-4db6-4a3e-44bbcfd36d68"
      },
      {
        "family": "inet",
        "host": "127.0.0.1",
        "port": 37183,
        "type": "prometheus",
        "uuid": "b1ba0893-930c-450a-a1a0-45ce88e25611"
      }
    ]

If the error can't be represented with the standard error codes
(`enoent`, `einval`, `eaccess` etc) the returned payload
contains more information just like create.

### Get/Set TLS properties

Key - TLS

If a body is provided it contains the new TLS configuration to use
described as a JSON object with the following layout:

    {
       "private key": "/path/to/file/containing/private/key",
       "certificate chain": "/path/to/file/containing/certificate/chain",
       "CA file", "/opt/couchbase/var/lib/couchbase/etc/CAfile.pem",
       "password" : "base64-encoded version of the password to decrypt private key",
       "minimum version" "TLS 1.2",
       "cipher list" : {
          "tls 1.2" : "ciphers for TLS <= 1.2",
          "tls 1.3" : "ciphers for TLS 1.3"
       },
       "cipher order" : true,
       "client cert auth" : "mandatory"
    }

On success the current TLS configuration is returned as JSON.

Note that the mapping rules for client certificate authentication remains
with the rest of the settings in `memcached.json`.
