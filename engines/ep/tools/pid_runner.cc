/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * pid_runner is a tool for running the PID object with a fake clock and is
 * useful for examining how changing P I D changes the output. The code may
 * need manual editing to best exercise various growth scenarios, it is not
 * intended to be a complete and finished tool.
 */

#include "pid_controller.h"

#include <getopt.h>
#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>

struct FauxClock {
    static std::chrono::milliseconds currentTime;
    static std::chrono::milliseconds rate;

    // now will change time
    static std::chrono::time_point<std::chrono::steady_clock> now() {
        currentTime += rate;
        return std::chrono::time_point<std::chrono::steady_clock>{
                std::chrono::steady_clock::duration{currentTime}};
    }

    // Doesn't tick
    static std::chrono::time_point<std::chrono::steady_clock> getTime() {
        return std::chrono::time_point<std::chrono::steady_clock>{
                std::chrono::steady_clock::duration{currentTime}};
    }
};

std::chrono::milliseconds FauxClock::currentTime{0};
std::chrono::milliseconds FauxClock::rate{1000};

static int errorUsage() {
    std::cerr <<
            R"(
usage: --p <proportional>    The 'p' term for the PID
       --i <integral>        The 'i' term for the PID
       --d <derivative>      The 'd' term for the PID
       --sp <setpoint>       The set point to compute the error value
       --interval <milliseconds>  How many milliseconds before PID updates
       --rate <milliseconds>      How many milliseconds for each step
       --steps <steps>       How many steps to run for

Example:

pid_runner --p 1.1 --i 0.01 --d 0.01 --sp 5.6 --interval 6000 --rate 1000 --steps 2000)"
              << std::endl;
    return 1;
}

int main(int argc, char* argv[]) {
    float p = 0.0;
    float i = 0.0;
    float d = 0.0;
    float setpoint = 0.0;
    int interval = 10000;
    int steps = 0;

    std::vector<option> long_options{
            {{"p", required_argument, nullptr, 'p'},
             {"i", required_argument, nullptr, 'i'},
             {"d", required_argument, nullptr, 'd'},
             {"sp", required_argument, nullptr, 's'},
             {"interval", required_argument, nullptr, 'I'},
             {"rate", required_argument, nullptr, 'r'},
             {"steps", required_argument, nullptr, 'S'},
             {"help", no_argument, nullptr, 0},
             {nullptr, 0, nullptr, 0}}};

    int cmd;
    while ((cmd = getopt_long(argc,
                              argv,
                              "p:i:d:s:I:r:S:g:",
                              long_options.data(),
                              nullptr)) != EOF) {
        switch (cmd) {
        case 'p':
            p = std::strtof(optarg, nullptr);
            break;
        case 'i':
            i = std::strtof(optarg, nullptr);
            break;
        case 'd':
            d = std::strtof(optarg, nullptr);
            break;
        case 's':
            setpoint = std::strtof(optarg, nullptr);
            break;
        case 'I':
            interval = std::strtoul(optarg, nullptr, 10);
            break;
        case 'r':
            FauxClock::rate = std::chrono::milliseconds{
                    std::strtoul(optarg, nullptr, 10)};
            break;
        case 'S':
            steps = std::strtoul(optarg, nullptr, 10);
            break;
        default:
            return errorUsage();
        }
    }

    std::cout << "PID P:" << p << " I:" << i << " D:" << d
              << " with SP:" << setpoint << std::endl;
    std::cout << "steps:" << steps << " rate:" << FauxClock::rate.count()
              << "ms interval:" << interval << "ms\n";

    // Currently coded to run the defragmenter time calculation, printing out
    // what the interval should be based on current PID output.
    float max = 10.0;
    float min = 0.0;
    // Currently coded to run step with PV at a fixed % above setpoint
    // (increments of 10%) The inner loop prints and breaks once the PID
    // has reduced the sleep interval from max to min
    for (float perc = 1.1; perc < 3.5; perc += 0.1) {
        auto pv = setpoint * perc;
        auto start = FauxClock::getTime();
        PIDController<FauxClock> pid{
                setpoint, p, i, d, std::chrono::milliseconds{interval}};
        for (int step = 0; step < steps; step++) {
            auto c = pid.step(pv);

            // Compute the sleep like the defragger does
            auto sleep = std::max(max + c, min);

            // Stop when we hit the min sleep so we can examine how many seconds
            // it took
            if (sleep <= min) {
                auto now = FauxClock::getTime() - start;
                using namespace std::chrono;
                auto secs = duration_cast<seconds>(now);
                auto hour = duration_cast<hours>(secs);
                secs -= duration_cast<seconds>(hour);
                auto mins = duration_cast<minutes>(secs);
                secs -= duration_cast<seconds>(mins);

                std::cout << hour.count() << "h:" << mins.count()
                          << "m:" << secs.count() << "s perc:" << perc
                          << " PV:" << pv << " output:" << c
                          << " sleep:" << sleep << "\n"
                          << "  PID state:" << pid << std::endl;
                break;
            }
        }
    }
    return 0;
}