/* win32.h
 *
 */

#ifndef WIN32_H
#define WIN32_H

#undef  _WIN32_WINNT
#define _WIN32_WINNT    0x0501        /* Needed to resolve getaddrinfo et al. */

#include <memcached/types.h>

#include <winsock2.h>
#include <ws2tcpip.h>

#include <stdio.h>
#include <io.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>

#define mkdir(a, b) mkdir(a)

#ifndef EWOULDBLOCK
#define EWOULDBLOCK        EAGAIN
#endif
#ifndef EAFNOSUPPORT
#define EAFNOSUPPORT       47
#endif
#ifndef EADDRINUSE
#define EADDRINUSE         WSAEADDRINUSE
#endif
#ifndef ENOTCONN
#define ENOTCONN           WSAENOTCONN
#endif
#ifndef ECONNRESET
#define ECONNRESET         WSAECONNRESET
#endif
#ifndef EAI_SYSTEM
#define EAI_SYSTEM         -11
#endif
#ifndef EMSGSIZE
#define EMSGSIZE WSAEMSGSIZE
#endif
#ifndef EINTR
#define EINTR WSAEINTR
#endif

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

#define IOV_MAX 20


typedef struct pollfd
{
    SOCKET fd;
    short events;
    short revents;
} pollfd_t;

#define POLLIN 0x0001
#define POLLOUT 0x0004
#define POLLERR 0x0008

static inline int poll(struct pollfd fds[], int nfds, int tmo)
{
    fd_set readfds, writefds, errorfds;
    FD_ZERO(&readfds);
    FD_ZERO(&writefds);
    FD_ZERO(&errorfds);

    for (int x= 0; x < nfds; ++x) {
        if (fds[x].events & (POLLIN | POLLOUT)) {
            if (fds[x].events & POLLIN) {
                FD_SET(fds[x].fd, &readfds);
            }
            if (fds[x].events & POLLOUT) {
                FD_SET(fds[x].fd, &writefds);
            }
        }
    }

    struct timeval timeout;
    timeout.tv_sec = tmo / 1000;
    timeout.tv_usec= (tmo % 1000) * 1000;
    struct timeval *tp= &timeout;
    if (tmo == -1) {
        tp = NULL;
    }
    int ret = select(/* IGNORED */ 0, &readfds, &writefds, &errorfds, tp);
    if (ret <= 0) {
        return ret;
    }

    for (int x = 0; x < nfds; ++x) {
        fds[x].revents = 0;
        if (FD_ISSET(fds[x].fd, &readfds)) {
            fds[x].revents |= POLLIN;
        }
        if (FD_ISSET(fds[x].fd, &writefds)) {
            fds[x].revents |= POLLOUT;
        }
        if (FD_ISSET(fds[x].fd, &errorfds)) {
            fds[x].revents |= POLLERR;
        }
    }

    return ret;
}

#ifndef __CYGWIN__

static inline int fsync(int fd)
{
    HANDLE h = (HANDLE) _get_osfhandle (fd);
    DWORD err;

    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }

    if (!FlushFileBuffers (h))  {
        err = GetLastError ();
        switch (err)  {
        case ERROR_INVALID_HANDLE:
            errno = EINVAL;
            break;
        default:
            errno = EIO;
        }
        return -1;
    }
    return 0;
}

static inline ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{
    off_t pos = lseek(fd, offset, SEEK_SET);
    if (pos < 0) {
        return pos;
    }

    return read(fd, buf, count);
}

static inline ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
{
    off_t pos = lseek(fd, offset, SEEK_SET);
    if (pos < 0) {
        return pos;
    }

    return write(fd, buf, count);
}

#endif

#endif /* WIN32_H */
