/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

    typedef enum {
        CONFIG_SUCCESS,
        CONFIG_INVALID_ARGUMENTS,
        CONFIG_NO_SUCH_FILE,
        CONFIG_OPEN_FAILED,
        CONFIG_MALLOC_FAILED,
        CONFIG_IO_ERROR,
        CONFIG_PARSE_ERROR
    } config_error_t;

    char *config_strerror(const char *file, config_error_t error);
    config_error_t config_load_file(const char *file, cJSON **json);

#ifdef __cplusplus
}
#endif
