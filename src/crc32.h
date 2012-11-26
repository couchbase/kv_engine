#ifndef SRC_CRC32_H_
#define SRC_CRC32_H_ 1

#include "config.h"

#include <stdint.h>

uint32_t crc32buf(uint8_t *buf, size_t len);

#endif  // SRC_CRC32_H_
