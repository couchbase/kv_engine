jemalloc_analyse.py
===================

Given the output of jemalloc's `malloc_stats_print`, analyse and show
utilization information. Insipired by:
http://www.canonware.com/pipermail/jemalloc-discuss/2013-November/000675.html

    Usage:

        cbstats allocator | jemalloc_analyse.py

If you're running against cbcollect_info's stats.log, then copy all
lines from

    --- Start jemalloc statistics ---

to

    --- End jemalloc statistics ---

(including those lines) into a file and pipe into the script.

Example output:

    === Stats for Arena 'merged' ===
    small allocation stats:
      bin  size (B)  regions     pages    allocated    allocated  cur runs         % of small                % of blame
                     per run   per run        items        bytes           utilization     frag memory (B)

        0         8      512         1      3333913     26671304      6512    100%      1%            1848       0%
        1        16      256         1          669        10704         5     52%      0%            9776       0%
        2        32      128         1      3366844    107739008     26415    100%      4%          456832      19%
        3        48      256         3         6949       333552        43     63%      0%          194832       8%
        4        64       64         1         3862       247168        71     85%      0%           43648       2%
        5        80      256         5      3337708    267016640     13039    100%      9%           22080       1%
        6        96      128         3         3355       322080        28     94%      0%           21984       1%
        7       112      256         7          133        14896         2     26%      0%           42448       2%
        8       128       32         1          840       107520        36     73%      0%           39936       2%
        9       160      128         5      3333890    533422400     26090    100%     17%          900800      37%
       10       192       64         3      3333858    640100736     52097    100%     21%           67200       3%
       11       224      128         7          806       180544        10     63%      0%          106176       4%
       12       256       16         1           23         5888         4     36%      0%           10496       0%
       13       320       64         5          193        61760         7     43%      0%           81600       3%
       14       384       32         3          909       349056        34     84%      0%           68736       3%
       15       448       64         7      3333430   1493376640     52085    100%     49%            4480       0%
       16       512        8         1         2962      1516544       382     97%      0%           48128       2%
       17       640       32         5          108        69120         7     48%      0%           74240       3%
       18       768       16         3            2         1536         1     12%      0%           10752       0%
       19       896       32         7           64        57344         6     33%      0%          114688       5%
       20      1024        4         1            8         8192         2    100%      0%               0       0%
       21      1280       16         5           40        51200         3     83%      0%           10240       0%
       22      1536        8         3          694      1065984        87    100%      0%            3072       0%
       23      1792       16         7           23        41216         3     48%      0%           44800       2%
       24      2048        2         1           63       129024        34     93%      0%           10240       0%
       25      2560        8         5           18        46080         3     75%      0%           15360       1%
       26      3072        4         3          685      2104320       172    100%      0%            9216       0%
        -      4096        1         1           12        49152        12       -       -               -        -
        -      8192        1         2           37       303104        37       -       -               -        -
        -     12288        1         3           12       147456        12       -       -               -        -
        -     16384        1         4           27       442368        27       -       -               -        -
        -     20480        1         5            8       163840         8       -       -               -        -
        -     28672        1         7          703     20156416       703       -       -               -        -
        -     32768        1         8            5       163840         5       -       -               -        -
        -     45056        1        11            3       135168         3       -       -               -        -
        -     69632        1        17            1        69632         1       -       -               -        -
        -    102400        1        25            1       102400         1       -       -               -        -
        -   2097152        1       512            2      4194304         2       -       -               -        -

      total                                                 2.9 GB                                    2.3 MB

        utilization = allocated / (size * regions_per_run * cur runs)
        % of small  = allocated / total allocated
        frag memory = (size * regions_per_run * cur runs) - allocated
        % of blame  = frag memory / total frag memory
