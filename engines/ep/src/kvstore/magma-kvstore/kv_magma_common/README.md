# magma-kvstore/kv_magma_common

Files in this directory are intended to comprise a library that is exported from
kv_engine to be re-used by other components i.e. magma. The particular use case
that this was written for is magma_dump which needs to be able to parse the
metadata that kv_engine store for each document to output it to the user.