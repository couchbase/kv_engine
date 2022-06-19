# Serverless static configuration

The file configuration.json contains static configuration which is used
in serverless deployments. It is read during startup to avoid having
to rebuild the software to change tunables.

The following parameters may be used:

`default_throttle_limit` This is an integer value containing the throttle
limit to set for each bucket as part of bucket creation.

`max_connections_per_bucket` This is an integer value containing the maximum
number of (external) connections to be permitted to select a given bucket.

`read_compute_unit_size` This is an integer value containing the size
(in bytes) of a compute unit used for reading.

`write_compute_unit_size` This is an integer value containing the size
(in bytes) of a compute unit used for writing.
