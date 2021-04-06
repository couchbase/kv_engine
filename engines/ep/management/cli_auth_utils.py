#!/usr/bin/env python3

"""

  Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included
in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
in that file, in accordance with the Business Source License, use of this
software will be governed by the Apache License, Version 2.0, included in
the file licenses/APL2.txt.

"""

import getpass
import inspect
import os
import sys

import clitool
import mc_bin_client
import memcacheConstants

from functools import wraps


def cmd_decorator(f):
    """Decorate a function with code to authenticate based on
    the following additional arguments passed by keyword:

    bucketName
    username
    password
    passwordFromStdin - if true, user will be prompted for password on stdin
    """

    @wraps(f)
    def g(*args, **kwargs):

        # check arguments are suitable for wrapped func
        mc = args[0]
        spec = inspect.getargspec(f)
        max_args = len(spec.args)
        defaults = len(spec.defaults) if spec.defaults else 0
        min_args = max_args - defaults

        if len(args) < min_args:
            print(("Error: too few arguments - command "
                                  "expected a minimum of %s but was passed "
                                  "%s: %s"
                                  % (min_args - 1, len(args) - 1, list(args[1:]))), file=sys.stderr)
            sys.exit(2)

        if spec.varargs is None:
            if len(args) > max_args:
                print(("Error: too many arguments - command "
                                      "expected a maximum of %s but was passed "
                                      "%s: %s"
                                      % (max_args - 1, len(args) - 1, list(args[1:]))), file=sys.stderr)
                sys.exit(2)

        # extract auth and bucket parameters (not passed to wrapped function)
        bucket = kwargs.pop('bucketName', None)
        username = kwargs.pop('username', None) or bucket
        cli_password = kwargs.pop('password', None)
        stdin_password = (getpass.getpass()
                          if kwargs.pop('passwordFromStdin', False)
                          else None)
        env_password = os.getenv("CB_PASSWORD", None)

        password = cli_password or stdin_password or env_password

        # try to auth
        if username is not None or password is not None:
            bucket = bucket or 'default'
            username = username or bucket
            password = password or ''
            try:
                mc.sasl_auth_plain(username, password)
            except mc_bin_client.MemcachedError:
                print("Authentication error for user:{0} bucket:{1}"
                       .format(username, bucket))
                sys.exit(1)

        # HELO
        mc.enable_xerror()
        mc.enable_collections()
        mc.hello("{0} {1}".format(os.path.split(sys.argv[0])[1],
                                os.getenv("EP_ENGINE_VERSION",
                                          "unknown version")))

        # call function for one or all buckets
        try:
            if kwargs.pop('allBuckets', None):
                buckets = mc.list_buckets()
                if not buckets:
                    print("No accessible buckets found")
                for bucket in buckets:
                    print('*' * 78)
                    print(bucket)
                    print()
                    mc.bucket_select(bucket)
                    f(*args, **kwargs)
            elif bucket is not None:
                mc.bucket_select(bucket)
                f(*args, **kwargs)
            else:
                f(*args, **kwargs)
        except mc_bin_client.ErrorEaccess:
            print("No access to bucket:{0} - permission denied "
                  "or bucket does not exist.".format(bucket))
            sys.exit(1)

    return g


def get_authed_clitool(extraUsage="", allBuckets=True):
    c = clitool.CliTool(extraUsage)

    if allBuckets:
        c.addFlag('-a', 'allBuckets', 'iterate over all buckets')
    c.addOption('-b', 'bucketName', 'the bucket to get stats from (Default: default)')
    c.addOption('-u', 'username', 'the user as which to authenticate (Default: bucketName)')
    c.addOption('-p', 'password', 'the password for the bucket if one exists')
    c.addFlag('-S', 'passwordFromStdin', 'read password from stdin')

    return c
