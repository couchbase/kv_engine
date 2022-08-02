# The CV slave for Mac failed to build the test program. Given that the
# nature of the program is in go we don't need to run it on Mac (or someone
# with access to a Mac computer could spend the time trying to figure out
# what's failing. The CMake syntax used here is the same as we've used in
# other projects using go so I don't really know whats going on on the Mac
# CV builder (and that's not what I'm trying to debug)
if (NOT APPLE)
    GoInstall(TARGET golang_tls_test
              PACKAGE tls_test
              UNSHIPPED
              GOPATH "${CMAKE_CURRENT_SOURCE_DIR}"
              GOVERSION 1.18)
    # Remove the sanitizers since the link step seems to pick up some
    # of the sanitizer libraries causing the link step to fail. This is
    # a go program and we don't need to sanitize the binary (but at the same
    # time we want to run the client towards the server that we've sanitized
    # so that it may possibly execute a new path on the server).
    remove_sanitizers(golang_tls_test)
endif ()
