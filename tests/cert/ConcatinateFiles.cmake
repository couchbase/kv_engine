# Read input file specified with
if (NOT DEFINED FILE1)
    message(FATAL_ERROR "FILE1 must be defined")
    endif()
if (NOT DEFINED FILE2)
    message(FATAL_ERROR "FILE2 must be defined")
endif()
if (NOT DEFINED DESTINATION)
    message(FATAL_ERROR "DESTINATION must be defined")
endif()

file(READ ${FILE1} CONTENT1)
file(READ ${FILE2} CONTENT2)
file(WRITE ${DESTINATION} ${CONTENT1}${CONTENT2})
