/* win32.h
 *
 */

#ifndef WIN32_H
#define WIN32_H

#undef  _WIN32_WINNT
#define _WIN32_WINNT    0x0501        /* Needed to resolve getaddrinfo et al. */

#include <winsock2.h>
#include <ws2tcpip.h>

#include <stdio.h>
#include <io.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>
#include <process.h>

#define EWOULDBLOCK        EAGAIN
#define EAFNOSUPPORT       47
#define EADDRINUSE         WSAEADDRINUSE
#define ENOTCONN           WSAENOTCONN
#define ECONNRESET         WSAECONNRESET
#define EAI_SYSTEM         -11

#define setsockopt(_socket, _level, _option_name, _option_value, _option_len) \
        setsockopt((_socket), (_level), (_option_name), (char*)(_option_value), (_option_len))
#endif /* WIN32_H */
