# mctrace

The `mctrace` tool controls and dumps a KV-Engine (Phosphor) trace
buffer as JSON, for an individual node.

## Description

`mctrace` stops tracing, retrieves the current trace buffer as a JSON
document (viewable with Chrome's trace viewer, `chrome://tracing`),
clears the dump on the server, and by default restarts tracing
afterwards.

It performs the whole dump over a single authenticated connection,
using the same connection handling (`McProgramGetopt`) as every other
C++ "mc" tool in this repository, so it supports TLS, JWT/mTLS
authentication and IPv6 in addition to plain SASL.

Unlike the simpler `kv_trace_dump` script (which only ever dumps
whatever trace is currently running), `mctrace` can also configure and
start a brand new trace, and can wait for an explicit capture window
before dumping, which makes it useful both for one-off "dump whatever
is currently being collected" invocations and for ad-hoc
troubleshooting sessions.

Once built and installed as part of Couchbase Server, `mctrace` is
available alongside the server's other command line tools:

| OS       | Location                                                                     |
|----------|-------------------------------------------------------------------------------------|
| Linux    | `/opt/couchbase/bin/mctrace`                                                   |
| Windows  | `C:\Program Files\Couchbase\Server\bin\mctrace.exe`                           |
| Mac OS X | `/Applications/Couchbase Server.app/Contents/Resources/couchbase-core/bin/mctrace` |

## Syntax

```
mctrace [common options] [options]
```

`host` defaults to `localhost:11210` (see the `-h`/`--host` option
below). Output defaults to standard output; use `-o`/`--output` to
write to a file instead.

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
| `--help`                         | Show the help message and exit.                                                                                                                                 |

### mctrace-specific options

| Option                          | Description                                                                                                                                                                                             |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-c, --config configuration`      | Specify the trace configuration to use on the server. This overrides the current configuration and starts a fresh trace; the previous configuration is NOT restored when the program terminates. Example: `"buffer-mode:ring;buffer-size:2000000;enabled-categories:*"`. |
| `-o, --output filename`           | Store the trace information in the named file, instead of writing it to standard output.                                                                                                              |
| `-w, --wait`                      | Wait until the user presses Ctrl-C before dumping the data. This clears any existing data on the server before waiting, so the resulting dump only contains events from the capture window that follows. |
| `--norestart`                     | Don't restart tracing after dumping the trace file.                                                                                                                                                     |

## Modes of operation

### 1. Dump the trace that is already running

If no configuration is given, `mctrace` expects tracing to already be
enabled on the server (e.g. started by a previous `mctrace -c`
invocation, or by some other means). It stops tracing, dumps the
buffer, clears it, and restarts tracing so collection continues
uninterrupted:

```
$ mctrace --host localhost:11210 --user Administrator -
```

If tracing is not currently enabled, this fails immediately:

```
$ mctrace --host localhost:11210 --user Administrator -
Trace is not running. Specify a configuration.
```

### 2. Start a new trace with a specific configuration

Use `-c`/`--config` to (re)configure and start tracing before
dumping. This is most useful combined with `-w` (see below) to define
an actual capture window; used on its own the dump will contain
whichever events happened to occur between starting the new trace and
the dump itself:

```
$ mctrace --host localhost:11210 --user Administrator \
    -c "buffer-mode:ring;buffer-size:2000000;enabled-categories:*" -
```

### 3. Capture a specific window of time interactively

Add `-w`/`--wait` to clear the buffer and wait for the user to press
Ctrl-C before dumping, so the dump only contains events from that
window. This can be combined with `-c` to also select which categories
are captured:

```
$ mctrace --host localhost:11210 --user Administrator -w -o trace.json
Press CTRL-C to stop trace
^C
```

### 4. Leave tracing stopped after dumping

By default `mctrace` restarts tracing after dumping so that ongoing
collection isn't interrupted. Pass `--norestart` to leave tracing
stopped instead:

```
$ mctrace --host localhost:11210 --user Administrator --norestart -
```

## Example

The following example dumps the current trace buffer to stdout:

```
$ mctrace --host localhost:11210 --user Administrator -
{ "clock_information" : { "ts" : 3049796622980.125,"system_clock" : "2026-09-08T18:42:53.513373+02:00" }, "traceEvents":[{"name":"thread_name","ph":"M","pid":78002,"tid":9475,"args":{"name":"ReaderPool3"}}, ...
```

To instead write the trace dump to a file:

```
$ mctrace --host localhost:11210 --user Administrator -o kv_trace.json
```
