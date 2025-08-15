#
# Generate a header file which contains the kvstore.fbs and events.fbs schemas
# as a string to enable the use of flatbuffers/idl.h methods without the need
# to load the schema file at run time.
#

file(READ
     ${DIR1}/src/collections/kvstore.fbs
     COLLECTIONS_KVSTORE_RAW_SCHEMA)

file(READ
     ${DIR1}/src/collections/events.fbs
     COLLECTIONS_VB_RAW_SCHEMA)

configure_file(${DIR1}/src/collections/flatbuffers_schema.in
               ${DIR2}/src/collections/flatbuffers_schema.cc)
