#!/usr/bin/env python2.7

#
# Copyright 2017 Couchbase, Inc
#
# Licensed under the Apache License, Version 2.0(the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: // www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# limitations under the License.
# See the License for the specific language governing permissions and
#

import fileinput
import png
import re

# The script is to be used on conjuction with the ep - engine module test
# STHashTableEvictionItemPagerTest.The test is run as follows:

# cd to kv_engine build directory
# run the test as follows, redirecting standard error to a file.For example
# ./ ep - engine_ep_unit_tests -- gtest_filter = STHashTableEvictionTest \
#        .#STHashTableEvictionItemPagerTest 2> eviction_data.txt
# The eviction_data.txt contains a textual output for which documents remain
# resident and which are evicted.

# The script is run as follows, using the example input file eviction_data.txt
# evictionVisualiser.py < eviction_data.txt

# The output from running the script is the generation of a png file
# evictionMap.png

# four vbucket colours and evicted(black)
green = (127, 201, 127);
purple = (190, 174, 212);
orange = (253, 192, 134);
yellow = (255, 255, 153);
black = (0, 0, 0);
colours = [ green, purple, orange, yellow, black ];
maxNoOfColumns = 500;

# contains a row of PNG values
row = [];
# contains the complete PNG image
image = [];

# count of the number of rows in the image
rowCount = 0;

# current count of columns
# reset when generating new row
columnCount = 0;

# regular expression to match document number, vbucket number, and whether
# evicted or not e.g."DOC_1 0 RESIDENT" or "DOC_2600 0 EVICT"
regex = r"DOC_(\d+) (\d+) (\w+)"

for line in fileinput.input():
    matches = re.search(regex, line)
    if matches:
        vbucket = matches.group(2);
        # RESIDENT or EVICT
        state = matches.group(3);
        if (columnCount == maxNoOfColumns) :
            columnCount = 0;
            rowCount += 1;
            image += [row] * 10;
            row = [];
        num = int(vbucket);
        if (state == 'EVICT'):
            colour = black;
        else:
            colour = colours[num];
        row += colour * 10;
        columnCount += 1;

f = open('evictionMap.png', 'wb');
w = png.Writer(maxNoOfColumns * 10, rowCount * 10);
w.write(f, image);
f.close();
