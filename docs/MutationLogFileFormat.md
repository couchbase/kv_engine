# Mutation log (access.log) file format

The file format is a block oriented format where the first
block contains a file header with the following header:

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|                            Version                            |
        +---------------+---------------+---------------+---------------+
       4|                           Block size                          |
        +---------------+---------------+---------------+---------------+
       8|                           Block count                         |
        +---------------+---------------+---------------+---------------+
      12|                           Read Write                          |
        +---------------+---------------+---------------+---------------+

All values are in network byte order.

* Version may be 1, 2, 3 or 4 (current version)
* Block size is the number of bytes in the block
* Block count is always set to 1
* ReadWrite used to be updated to 1 as part of open with Write permission and
  set back to 0 when successfully closing the file (not used anymore and
  always set to 0)

Following the header the file is padded up to the block size so the first
block containing data is located at the file offset `block size`.

The data block use the following format

      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|            checksum           | Number of entries in the block|
        +---------------+---------------+---------------+---------------+
       4|                                                               |
        |                         Data entries                          |
        |                                                               |
        +---------------+---------------+---------------+---------------+

The checksum and number of entries are stored in network byte order.

* Checksum is the least significant bytes of a crc32c of the entire block
  (except for the two bytes for the checksum)
* Number of entries represents the number of entries stored within this
  block.
