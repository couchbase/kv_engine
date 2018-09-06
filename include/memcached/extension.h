/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include "spdlog/common.h"

namespace spdlog {
class logger;
}

typedef enum {
    EXTENSION_LOG_TRACE = 0,
    EXTENSION_LOG_DEBUG,
    EXTENSION_LOG_INFO,
    EXTENSION_LOG_NOTICE,
    EXTENSION_LOG_WARNING,
    EXTENSION_LOG_FATAL
} EXTENSION_LOG_LEVEL;

typedef struct {
    /**
     * Pointer to an spdlog getter function used by memcached
     */
    spdlog::logger* (*spdlogGetter)(void);
} EXTENSION_SPDLOG_GETTER;

struct ServerLogIface {
    virtual ~ServerLogIface() = default;
    virtual EXTENSION_SPDLOG_GETTER* get_spdlogger() = 0;
    virtual void set_level(spdlog::level::level_enum severity) = 0;
};
