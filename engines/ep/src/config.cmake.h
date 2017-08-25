#pragma once

/* Header files */
#cmakedefine HAVE_ARPA_INET_H ${HAVE_ARPA_INET_H}
#cmakedefine HAVE_NETDB_H ${HAVE_NETDB_H}
#cmakedefine HAVE_NETINET_IN_H ${HAVE_NETINET_IN_H}
#cmakedefine HAVE_NETINET_TCP_H ${HAVE_NETINET_TCP_H}
#cmakedefine HAVE_POLL_H ${HAVE_POLL_H}
#cmakedefine HAVE_SYS_SOCKET_H ${HAVE_SYS_SOCKET_H}
#cmakedefine HAVE_SYS_TIME_H ${HAVE_SYS_TIME_H}
#cmakedefine HAVE_SCHED_H ${HAVE_SCHED_H}
#cmakedefine HAVE_UNISTD_H ${HAVE_UNISTD_H}

/* various */
#define VERSION "${EP_ENGINE_VERSION}"

#include "config_static.h"
