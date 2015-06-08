FIND_PROGRAM(A2X_EXECUTABLE a2x)
IF (A2X_EXECUTABLE)
  ADD_CUSTOM_TARGET(memcached-manpage
    COMMAND ${A2X_EXECUTABLE} -L --doctype manpage --format manpage
                      ${CMAKE_CURRENT_SOURCE_DIR}/man4/memcached.json.4.txt
    COMMAND ${A2X_EXECUTABLE} -L --doctype manpage --format pdf
                      ${CMAKE_CURRENT_SOURCE_DIR}/man4/memcached.json.4.txt
    VERBATIM)

  IF (WIN32)
    INSTALL(FILES man4/memcached.json.4.pdf
            DESTINATION doc)
  ELSE (WIN32)
    INSTALL(FILES man4/memcached.json.4
            DESTINATION man/man4)
  ENDIF(WIN32)
ELSE (A2X_EXECUTABLE)
  MESSAGE(STATUS "Can't build manual page without a2x")
ENDIF(A2X_EXECUTABLE)
