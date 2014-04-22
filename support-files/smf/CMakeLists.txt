CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchbase.xml.in
               ${CMAKE_CURRENT_BINARY_DIR}/couchbase.xml)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/couchbase.in
               ${CMAKE_CURRENT_BINARY_DIR}/couchbase)


INSTALL(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/couchbase.xml
        DESTINATION smf)

INSTALL(PROGRAMS
        ${CMAKE_CURRENT_BINARY_DIR}/couchbase
        DESTINATION smf)
