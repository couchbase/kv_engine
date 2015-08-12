ADD_EXECUTABLE(cbtrace channel.h main.cc memorymap.cc memorymap.h pcap.cc pcap.h)
TARGET_LINK_LIBRARIES(cbtrace mcd_util platform dirutils ${COUCHBASE_NETWORK_LIBS})

# The current state of the program is not good engough for it to be a part
# of the product (it was written to solve a single problem and has not been
# hardened to handle cases like tcp retransmit etc). Lets keep it in here
# for us to continue to fix bugs and slowly make it usable.
#
#INSTALL(TARGETS cbtrace
#        RUNTIME DESTINATION bin)
