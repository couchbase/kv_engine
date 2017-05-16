# Project containing public headers exported by memcached.
# Exists as a separate project from KVEngine to avoid circular
# dependancies (e.g. couchstore depends on
# <memcached/protocol_binary.h>, but ep.so depends on couchstore).
#
# Example usage:
#   INCLUDE_DIRECTORIES(${KVEngine_API_SOUCRE_DIR}/include)
#
project(KVEngineAPI)
