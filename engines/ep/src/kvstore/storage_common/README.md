# kvstore/storage_common

Files in this directory are intended to comprise a library that is exported from
kv_engine to be re-used by other components i.e. couchstore and magma. The
particular use case that this was written for is the couch_dbdump and magma_dump
tools which need to be able to parse the metadata that kv_engine stores (in
particular that which is stored in local documents in a format common to both
KVStores).