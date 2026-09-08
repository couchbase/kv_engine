# mcstat

The `mcstat` tool provides Data Service statistics, for an
individual node.

## Description

`mcstat` connects directly to a node over the Memcached Binary
Protocol and issues the `stat` command, requesting one of the many
named stat groups the server exposes (see [Statkeys](#statkeys)
below). Some stat groups are bucket-scoped and require `-b`/`--bucket`
(or `-a`/`--all-buckets`); some require the `Stats` privilege.

It is built using the same connection handling (`McProgramGetopt`) as
every other C++ "mc" tool in this repository, so it supports TLS,
JWT/mTLS authentication and IPv6 in addition to plain SASL. Beyond
simply printing raw key/value pairs, `mcstat` negotiates Collections
support and fetches the server's error map once per connection (reused
for the lifetime of the connection, rather than re-fetched per
request), and has specialized pretty-printers for a number of stat
groups: timing histograms are rendered as sparkline bars (UTF-8 by
default, or plain ASCII via `--disable-utf8`), and `hash`,
`dispatcher`, `responses` and `tasks`/`tasks-all` are rendered as
formatted tables rather than raw key/value pairs.

Once built and installed as part of Couchbase Server, `mcstat` is
available alongside the server's other command line tools:

| OS       | Location                                                                   |
|----------|--------------------------------------------------------------------------|
| Linux    | `/opt/couchbase/bin/mcstat`                                               |
| Windows  | `C:\Program Files\Couchbase\Server\bin\mcstat.exe`                       |
| Mac OS X | `/Applications/Couchbase Server.app/Contents/Resources/couchbase-core/bin/mcstat` |

## Syntax

```
mcstat [common options] [options] statkey [arguments to statkey]
```

`host` defaults to `localhost:11210` (see the `-h`/`--host` option
below); `statkey` is one of the stat groups listed under
[Statkeys](#statkeys), and some statkeys accept further arguments
(e.g. a vbucket ID, a key name, or a scope/collection name or ID).

### Common options

These options are shared by every C++ "mc" tool in this repository:

| Option                          | Description                                                                                                                                                     |
|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-h, --host hostname[:port]`     | The host (with an optional port) to connect to. For IPv6 use `[address]:port`.                                                                                 |
| `-p, --port number`              | The port number to connect to.                                                                                                                                  |
| `-u, --user username`            | The name of the user to authenticate as.                                                                                                                        |
| `-P, --password password`        | The password to use for authentication. Use `-` to read the password from `stdin`, or set the `CB_PASSWORD` environment variable.                             |
| `--tls[=cert,key[,castore]]`     | Use TLS. `cert`/`key` (optional) provide a client certificate and key; a non-default CA store may optionally be provided.                                      |
| `--no-peer-verify`               | Disable verification of the peer's certificate.                                                                                                                 |
| `-4, --ipv4`                     | Connect over IPv4.                                                                                                                                               |
| `-6, --ipv6`                     | Connect over IPv6.                                                                                                                                               |
| `--sasl_mechanism mechanism`     | Use the provided mechanism for SASL authentication.                                                                                                             |
| `--token-auth`                   | Use JWT token authentication.                                                                                                                                   |
| `--token-lifetime value`         | The lifetime to use for the token (default: `1m`).                                                                                                              |
| `--token-skeleton-file value`    | Use the provided file as a skeleton for tokens.                                                                                                                 |
| `--token-passphrase-file value`  | Use the provided file containing the token passphrase.                                                                                                          |
| `-n, --no-color`                 | Disable colored output.                                                                                                                                         |
| `--version`                      | Print the program version and exit.                                                                                                                             |

### mcstat-specific options

| Option                      | Description                                                                              |
|-------------------------------|---------------------------------------------------------------------------------------------|
| `-j, --json[=value]`          | Print the result in JSON (pretty-printing is no longer supported).                          |
| `-b, --bucket bucketname`     | The bucket whose statistics are to be retrieved.                                             |
| `-a, --all-buckets`           | Get the list of buckets from the node and display stats on a per-bucket basis. Mutually exclusive with `-b`/`--bucket`. |
| `--sort`                      | Sort the output (only valid for non-JSON output).                                            |
| `--disable-utf8`              | Render timing histogram bars with plain ASCII instead of UTF-8 sparkline characters (UTF-8 is used by default). |
| `--help[=statkey]`            | Show the help message, or (with `=statkey`) a description of a specific statkey.             |

## Statkeys

`statkey` is one of a large number of named stat groups, some
bucket-scoped (marked `B`) and some requiring the `Stats` privilege
(marked `P`) - for example `timings`, `vbucket`, `dcp`, `checkpoint`,
`hash`, `tasks`, `warmup`, `key <key> <vbid>` and many more. Run
`mcstat --help=statkey` for the full, up to date list with
descriptions, and `mcstat --help=<statkey>` for a description of one
specific statkey.

## Example

The following example retrieves the `timings` statistics for the
`default` bucket, on the current host:

```
$ mcstat --user trond --password asdfasdf --bucket default timings
...
The following data is collected for "expiry_pager"
[  0.00 -  53.00]us (0.0000%)   1| ██████████████▋
[ 53.00 -  63.00]us (10.0000%)   2| █████████████████████████████▎
...
```

Pass `--disable-utf8` to render the same histogram bars with plain
ASCII instead:

```
$ mcstat --user trond --password asdfasdf --bucket default --disable-utf8 timings
...
[  0.00 -  53.00]us (0.0000%)   1| ##############
[ 53.00 -  63.00]us (10.0000%)   2| #############################
...
```
