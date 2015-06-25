ADD_SUBDIRECTORY(basic)
# Under threadsanitizer these tests are pretty slow, and they don't
# really expose anything interesting from a threading pov, so skip
# them if ThreadSanitizer enabled.
IF(NOT CB_THREADSANITIZER)
    ADD_SUBDIRECTORY(breakdancer)
ENDIF()
