/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_OPENSSL_H
#define MEMCACHED_OPENSSL_H

/*
 * Unfortunately Apple deprecated the use of OpenSSL in (at least) Mavericks
 * and added attributes to the methods in the header causing the code to
 * emit a ton of warnings. I guess I should wrap all of the methods we're
 * using into another library so that I don't have to disable the deprecation
 * warnings for other code. We know of this one, but we don't want to make
 * an apple specific version (I guess the other option would be to drop the
 * support for ssl on mac ;-)
 */
#if defined(__APPLE__) && defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/err.h>

#endif
