#
# Generate a header file which contains the kvstore.fbs schema as a string
# to enable the use of flatbuffers/idl.h methods without the need to load the
# schema file at run time.
#

file(READ
     ${DIR1}/src/collections/kvstore.fbs
     COLLECTIONS_EVENTS_RAW_SCHEMA)

configure_file(${DIR1}/src/collections/kvstore.in
               ${DIR2}/src/collections/kvstore_flatbuffers_schema.cc)
