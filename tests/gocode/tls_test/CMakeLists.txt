GoModBuild(TARGET golang_tls_test
           PACKAGE tls_test
           GOVERSION 1.18.2
           OUTPUT tls_test)
# Remove the sanitizers since the link step seems to pick up some
# of the sanitizer libraries causing the link step to fail. This is
# a go program and we don't need to sanitize the binary (but at the same
# time we want to run the client towards the server that we've sanitized
# so that it may possibly execute a new path on the server).
remove_sanitizers(golang_tls_test)
