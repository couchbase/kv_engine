#ifndef SASL_DEFS_H
#define SASL_DEFS_H 1

/* Longest one I could find was ``9798-U-RSA-SHA1-ENC'' */
#define MAX_SASL_MECH_LEN 32

#include "isasl.h"
void init_sasl(void);
void shutdown_sasl(void);

#endif /* SASL_DEFS_H */
