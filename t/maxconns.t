#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 16;

use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;


# start up a server with 10 maximum connections
my $server = new_memcached('-c 20');
my $sock = $server->sock;
my @sockets;

ok(defined($sock), 'Connection 0');
push (@sockets, $sock);


foreach my $conn (1..10) {
  $sock = $server->new_sock;
  ok(defined($sock), "Made connection $conn");
  push(@sockets, $sock);
}
@sockets = [];
$server->DESTROY;

$server = new_memcached('-c20 -l127.0.0.1:11210:5,127.0.0.1:11211:5', 11210);
foreach my $conn (1..5) {
  # Open the connection with port 11210
  $sock = $server->new_sock;
  ok(defined($sock), "Made connection $conn");
  push(@sockets, $sock);
}
