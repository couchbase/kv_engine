Welcome to the Couchbase _memcached_ project.

This started as Couchbase's fork of the upstream `memcached` project.
It has subsequently evolved since then, so while it shares a name with
the upstream project many other things have changed, apart from the
name :) For now it's simpler to consider this as the frontend of the
Couchbase key-value engine.  The primary backend of KV-engine is the
eventually persistent engine -
[ep-engine](https://github.com/couchbase/ep-engine).

# Architecture

* [KV-Engine Architecture](docs/Architecture.md)
* [CBSASL](cbsasl/CBSASL.md)
* [Audit](auditd/README.md)
* [Error Handling Best Practices](docs/ErrorHandling.md)
* [Event Tracing / Phosphor](docs/Tracing.md)

# Protocols

* [Greenstack](protocol/Greenstack/README.md)
* [Memcached Binary Protocol](docs/BinaryProtocol.md)
    * [SASL](docs/sasl.md)
    * [TAP](docs/TAP.md)

# Tools

* [Analyze jemalloc memory statistics](scripts/jemalloc/jemalloc_analyse.md)
