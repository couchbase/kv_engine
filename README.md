# KV-Engine

Welcome to the Couchbase _KV-Engine_ project.

This started as Couchbase's fork of the upstream `memcached` project,
but has substantially evolved since then. It contains the bulk of the
code for the Key/Value service of Couchbase Server.

## Repository Layout

Areas of interest in the repository:

* [`include/`](include/) - Public header files.
* `daemon/` - Source for the main daemon process (also known as the
  _server_). This is where [`main()`](daemon/memcached.cc) lives.
* `engines/` - Source for the different engines (aka bucket types)
  supported. Includes:
    * [`default engine`](engines/default_engine/) - The original
      _memcache_ engine. Powers the _Memcache_ bucket type.
    * [`ep`](engines/ep/) - _Eventually Persistent_ Engine. Powers the
    _Couchbase_ and _Ephemeral_ bucket types.
    * [`ewouldblock engine`](engines/ewouldblock_engine) - Test engine
      which interposes a real engine and can perform various types of
      error-injection.
* `tests/` - Test cases.
* `licenses/` - The various licenses in use

## Building

KV-Engine has a number of external dependancies, as such it should be
built via the Couchbase
[top-level Makefile](https://github.com/couchbase/tlm).

## Documentation

### Architecture

* [KV-Engine Architecture](docs/Architecture.md)
* [CBSASL](cbsasl/CBSASL.md)
* [Audit](auditd/README.md)
* [Event Tracing / Phosphor](docs/Tracing.md)
* [Document attributes](docs/Document.md)
* [Environment variables](docs/EnvironmentVariables.md)
* [Role Based Access Control (RBAC)](docs/rbac.md)
* [SSL Client Certificate](docs/ssl_client_cert.md)
* [DCP](docs/dcp/README.md)
* [Memcached configuration file](docs/memcached.json.adoc)
* [Network interfaces](docs/NetworkInterface.md)

### Protocols

* [Memcached Binary Protocol](docs/BinaryProtocol.md)
    * [SASL](docs/sasl.md)
    * [Duplex mode](docs/Duplex.md)
    * [External Authentication Provider](docs/ExternalAuthProvider.md)
    * [DCP](docs/dcp/documentation/protocol.md)

### Policies / Guidelines

* [Coding Standards](docs/CodingStandards.rst)
* [Error Handling Best Practices](docs/ErrorHandling.md)

## Tools

* [Analyze jemalloc memory statistics](scripts/jemalloc/README.md)

## Related Projects

While the bulk of code making up KV-Engine is in this repo, there are
a number of other repositories which contribute to the final program:

* [`couchbase/couchstore`](https://github.com/couchbase/couchstore) -
  Couchbase storage file library.
* [`couchbase/phosphor`](https://github.com/couchbase/phosphor) -
  Phosphor: high performance event tracing framework.
* [`couchbase/platform`](https://github.com/couchbase/platform) -
  Platform abstraction layer.
* [`couchbase/subjson`](https://github.com/couchbase/subjson) -
  subjson - quickly manipulate JSON subfields.
