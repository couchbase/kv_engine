#!/usr/bin/env python3

"""Given the output of jemalloc's `malloc_stats_print`, analyse and show
utilization information. Insipired by:
http://www.canonware.com/pipermail/jemalloc-discuss/2013-November/000675.html
"""

import re
import sys


def sizeof_fmt(num):
    for unit in ('bytes', 'KB', 'MB', 'GB', 'TB'):
        if num < 1024.0:
            return '{0:3.1f} {1}'.format(num, unit)
        num /= 1024.0


def main():
    if len(sys.argv) < 2 and sys.stdin.isatty():
        print("""Usage: {0} <jemalloc stats file>
       cat <jemalloc stats file> | {0}

Given the output of jemalloc's `malloc_stats_print`, analyse and show
utilization and fragmentation information.""".format(sys.argv[0]),
              file=sys.stderr)
        return 1

    with open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin as stats:
        line = stats.readline()
        while line:
            if line.startswith('Merged arenas stats:'):
                calc_bin_stats(stats, "merged")
            elif line.startswith('arenas['):
                m = re.search('arenas\\[(\\d+)\\]', line)
                calc_bin_stats(stats, m.group(1))

            line = stats.readline()

    # Some explanation of the table(s)
    print("""
    utilization = allocated / (size * regions_per_run * cur runs)
    % of small  = allocated / total allocated
    frag memory = (size * regions_per_run * cur runs) - allocated
    % of blame  = frag memory / total frag memory
""")


def calc_bin_stats(stats, arena_ID):
    FMT = ('  {0:>3} {1:>9} {2:>8} {3:>9} {4:>12} {5:>12} {6:>9} {7:>7} '
           '{8:>7} {9:>15} {10:>8}')

    # Scan down to the table
    line = stats.readline()
    while not line.startswith('bins:'):
        line = stats.readline()
    headers = line.split()[1:]
    line = stats.readline()
    if line.startswith('['):
        line = stats.readline()

    # Extract the raw stats, recording in a list of size classes.
    output_cropped = False
    classes = list()
    while not line.startswith('large:'):
        if line.strip() == "---":
            line = stats.readline()
            continue

        fields = [int(x) if x.isdigit() else float(x)
                  for x in line.split()]
        c = dict(list(zip(headers, fields)))

        # jemalloc 5.0.0 onwards renames `runs` to `slabs` for small
        # allocations. Copy back to the old name to allow calculations to work.
        if 'curslabs' in c:
            c['curruns'] = c['curslabs']

        # Derive some stats from each class, additional ones (see below) need
        # totals...
        try:
            c['utilization'] = c['allocated'] / (c['size'] * c['regs'] *
                                                 c['curruns'])
        except ZeroDivisionError:
            c['utilization'] = 1
        c['alloc_items'] = int(c['allocated'] / c['size'])
        c['frag_memory'] = ((c['size'] * c['regs'] * c['curruns']) -
                            c['allocated'])
        c['small'] = True
        classes.append(c)

        line = stats.readline()

    if line.startswith('large:'):
        # Different format for large/huge allocations.
        headers = line.split()[1:]
        line = stats.readline()
        while (line.strip() and
               not (line.startswith('--- End jemalloc statistics ---') or
                    line.startswith('extents:'))):
            if line.startswith(
                    '=== Exceeded buffer size - output cropped ==='):
                output_cropped = True
                break

            if (line.startswith('[') or
                line.startswith('huge:') or
                    line.strip() == "---"):
                line = stats.readline()
                continue
            fields = [int(x) for x in line.split()]
            c = dict(list(zip(headers, fields)))

            # jemalloc 5.0.0 onwards renames `curruns` to `l(large)extents` for
            # large allocations. Copy back to the old name to allow
            # calculations to work.
            if 'curlextents' in c:
                c['curruns'] = c['curlextents']

            c['bin'] = '-'
            c['regs'] = 1  # Only one region per large allocation
            c['pgs'] = c['pages'] if 'pages' in c else c['size'] // 4096
            c['utilization'] = 1
            c['allocated'] = (c['allocated'] if 'allocated' in c else
                              c['size'] * c['curruns'])
            c['alloc_items'] = c['curruns']
            c['frag_memory'] = 0
            c['small'] = False
            classes.append(c)

            line = stats.readline()

    # Calculate totals
    total_allocated = sum([c['allocated'] for c in classes])
    total_allocated_small = sum([c['allocated'] for c in classes
                                 if c['small']])
    total_frag_memory = sum([c['frag_memory'] for c in classes])

    print("=== Stats for Arena '{0}' ===".format(arena_ID))
    print("small allocation stats:")
    print(FMT.format('bin', 'size (B)', 'regions', 'pages', 'allocated',
                     'allocated', 'cur runs', '', '% of small',
                     '               % of blame', ''))
    print(FMT.format('', '', 'per run', 'per run', 'items', 'bytes', '',
                     'utilization    ', 'frag memory (B)', '', ''))
    print()

    # Finally, calculate per-class stats which need the totals.
    for c in classes:
        if c['small']:
            utilization = '{0:.0f}%'.format(c['utilization'] * 100)
            pct_of_small = '{0:.0f}%'.format(
                c['allocated'] * 100 / total_allocated_small)
            frag_memory = c['frag_memory']
            pct_of_blame = '{0:.0f}%'.format(
                c['frag_memory'] * 100 / total_frag_memory)
        else:
            utilization = '-'
            pct_of_small = '-'
            frag_memory = '-'
            pct_of_blame = '-'

        # Skip empty runs
        if c['curruns'] == 0:
            continue

        print(FMT.format(c['bin'] if 'bin' in c else c['ind'],
                         c['size'], c['regs'], c['pgs'],
                         c['alloc_items'], c['allocated'], c['curruns'],
                         utilization,
                         pct_of_small,
                         frag_memory,
                         pct_of_blame))

    print(FMT.format('total', '', '', '', '', sizeof_fmt(total_allocated), '',
                     '', '', sizeof_fmt(total_frag_memory), ''))

    if output_cropped:
        print()
        print("WARNING: Output was cropped due to exceeding buffer size - not "
              "all sizes classes may be listed.")


if __name__ == '__main__':
    main()
