# Serverless static configuration

The file configuration.json contains static configuration which is used
in serverless deployments. It is read during startup to avoid having
to rebuild the software to change tunables.

The following parameters may be used:

`default_throttle_reserved_units` This is an integer value containing the
throttle reserved units to set for each bucket as part of bucket creation. 
This is the reserved (and minimum) capacity for a tenant.

`default_throttle_hard_limit` This is an integer value containing the throttle
hard limit to set for each bucket as part of bucket creation. This is the
maximum capacity for a tenant (if set to 0 no limit is used).

`max_connections_per_bucket` This is an integer value containing the maximum
number of (external) connections to be permitted to select a given bucket.

`read_unit_size` This is an integer value containing the size (in
bytes) of the unit used for reading.

`write_unit_size` This is an integer value containing the size (in
bytes) of the unit used for writing.

`node_capacity` This is an integer value containing the capacity of
read/write units per sec.
