#!/bin/sh

set -e

if [ -d .git ]
then
  perl config/version.pl || die "Failed to run config/version.pl"
fi

autoreconf -i --force
