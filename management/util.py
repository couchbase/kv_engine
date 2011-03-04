#!/usr/bin/env python

import glob

def expand_file_pattern(file_pattern):
    """Returns an unused filename based on a file pattern,
       like "/some/where/backup-%.mbb", replacing the optional
       placeholder character ('%') with the next, unused, zero-padded number.
       Example return value would be "/some/where/backup-00000.mbb".
    """
    not_hash = file_pattern.split('%')
    if len(not_hash) == 1:
        return file_pattern
    if len(not_hash) != 2:
        raise Exception("file pattern should have"
                        + " at most one '%' placeholder character: "
                        + file_pattern)

    max = -1
    existing = glob.glob(file_pattern.replace('%', '*'))
    for e in existing:
        for s in not_hash:
            e = e.replace(s, '')
        n = int(e)
        if max < n:
            max = n

    return file_pattern.replace('%', str(max + 1).zfill(5))

