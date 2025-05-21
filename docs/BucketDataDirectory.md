# `$datadir/{bucket-name` / `$datadir/{bucked-uuid}`

The data directory contains files created by multiple teams. Files or
directories in this directory are owned by KV by default, except as
listed in the “existing files” table below.

If teams wish to create files under the bucket directory, they should

1. create a directory that represents their ownership or the particular use case
2. let the KV team know (to update the table below)
3. place whatever files they would like in that directory.

## Existing files

| Kind of file              | File Name Pattern          | Owner / Team |
|---------------------------|----------------------------|--------------|
| Couchstore files          | `n.couch.m`                | KV           |
| Views Definitions         | `master.couch.m`           | Views        |
| Flush sequence # file     | `flushseq`                 | NS-Server    |
| Magma files / directories | `magma*`                   | Storage      |
| Stats.json                | `stats.json*`              | KV           |
| Data Encryption Keys      | `deks/`                    | NS-Server    |
| Access log                | `access.log.n[.old][.cef]` | KV           |
| Cluster management        | `cm/`                      | NS-Server    |
| Snapshots                 | `snapshots/`               | KV           |
