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

static inline void mapErr(int error) {
    switch(error) {
        default:
            errno = ECONNRESET;
            break;
        case WSAEPFNOSUPPORT:
            errno = EAFNOSUPPORT;
            break;
        case WSA_IO_PENDING:
        case WSATRY_AGAIN:
            errno = EAGAIN;
            break;
        case WSAEWOULDBLOCK:
            errno = EWOULDBLOCK;
            break;
        case WSAEMSGSIZE:
            errno = E2BIG;
            break;
        case WSAECONNRESET:
            errno = 0;
            break;
    }
}


#define recv(a,b,c,d) mem_recv(a,b,c,d)

static inline size_t mem_recv(int s, void *buf, int len, int unused)
{
    DWORD flags = 0;
    DWORD dwBufferCount;
    WSABUF wsabuf = { len, (char *)buf };
    int error;

    if(WSARecv((SOCKET)s, &wsabuf, 1, &dwBufferCount, &flags,
              NULL, NULL) == 0) {
        return dwBufferCount;
    }
    error = WSAGetLastError();
    if (error == WSAECONNRESET) {
        return 0;
    }
    mapErr(error);
    return -1;
}

#endif /* WIN32_H */
