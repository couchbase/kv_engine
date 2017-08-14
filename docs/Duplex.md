# Duplex mode

Initially all connections to memcached is command-response driven, and
all traffic is _always_ initiated by the client. This means that the
server does not have a way to notify the client (or request
information / actions). It is however possible to toggle the
connection to memcached into duplex mode by using the
[HELO](BinaryProtocol.md#0x1f-helo) command. When running in a duplex
mode the server may send commands to the client at any time. The
client must reply to the command, just like a normal command being
sent from the client to the server.

## Packet sequence

The smallest unit which may appear on the socket in each direction is
a complete packet (24 bytes fixed header with an additional variable
length body) See [Packet Structure](BinaryProtocol.md#packet-structure).

The receiver should inspect the magic byte in the header to determine
the packet type and how to interpret the content.

    +--------+-----------------------------------------------+
    | Magic  | Direction                                     |
    +--------+-----------------------------------------------+
    |  0x80  | May be sent from the client to the server     |
    |  0x81  | May be sent from the server to the client     |
    |        | as a response for the command with magic 0x81 |
    |  0x82  | May be sent from the server to the client     |
    |  0x83  | May be sent from the client to the server     |
    |        | as a response for the command with magic 0x82 |
    +--------+-----------------------------------------------+

Connections used for DCP traffic behaves a bit different, where both
parties may send packets with magic 0x80 and 0x81.

Note that each end may inject their commands at any point in time, and
the receiving side shouldn't assume anything about the handling of its
own commands by the appearance of a command sent from the other end
(unless explicitly told so by the command it receives). The stats
command for instance use multiple response packets with a final
"empty" packet as it's termination packet.  It is perfectly legal for
the server to inject commands in the middle of all of the response
packets.
