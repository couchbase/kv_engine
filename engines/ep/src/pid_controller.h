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

#pragma once

#include <chrono>
#include <iosfwd>

/**
 * https://en.wikipedia.org/wiki/PID_controller.
 *
 * This PIDControllerImpl is a simple implementation of the PID algorithm. It is
 * simple in that other PID implementations may include a variety of extra
 * functionality that for example allow for runtime changes to SP, P, I and D
 * without disruption to the output. In this implementation SP, P, I and D can
 * only be changed with a complete reset of the object. This PID uses
 * milliseconds for the granularity of the interval, but could use other types
 * such as fractional seconds (e.g. 10.0), this really then just affects the
 * scale of the I and D terms.
 *
 * As recommended by well referenced blog on the topic of PIDs all calculations
 * are float
 * http://brettbeauregard.com/blog/2011/04/improving-the-beginners-pid-introduction/
 */
class PIDControllerImpl {
public:
    /**
     * @param setPoint The PID's set point (SP)
     * @param kp The PID's proportional term
     * @param ki The PID's integral term
     * @param kd The PID's derivative term
     * @param dt The PID's interval, only recomputes when this amount of time
     *           has elapsed since construction or step.
     * @param now The current time to initialise the PID with (for calculating
     *            step intervals.
     */
    PIDControllerImpl(float setPoint,
                      float kp,
                      float ki,
                      float kd,
                      std::chrono::milliseconds dt,
                      std::chrono::time_point<std::chrono::steady_clock> now);

    /**
     * Step the controller - comparing now with the previous step and if dt
     * has elapsed calculate the new error and generate a new output.
     *
     * @param current The current reading of the process variable
     * @param now The time point for 'now'
     * @return the PID's computed output
     */
    float step(float current,
               std::chrono::time_point<std::chrono::steady_clock> now);

    /**
     * Reset the PID state.
     * The current integral, previousError and output are all set to zero.
     */
    void reset();

private:
    float kp{0.0};
    float ki{0.0};
    float kd{0.0};
    std::chrono::milliseconds dt{0};
    float integral{0.0};
    float setPoint{0.0};
    float previousError{0.0};
    float output{0.0};
    std::chrono::time_point<std::chrono::steady_clock> lastStep;
    friend std::ostream& operator<<(std::ostream&, const PIDControllerImpl&);
};

/**
 * PIDController wraps PIDControllerImpl providing a way to replace time for
 * test and simulation purposes
 * @tparam Clock a clock to call ::now(), allowing for replacement of time
 */
template <class Clock = std::chrono::steady_clock>
class PIDController {
public:
    PIDController(float setPoint,
                  float kp,
                  float ki,
                  float kd,
                  std::chrono::milliseconds dt)
        : impl(setPoint, kp, ki, kd, dt, Clock::now()) {
    }

    /**
     * Step the controller and recompute if dt time has elapsed
     *
     * @param current The current reading of the process variable
     * @return the PID's computed output
     */
    float step(float current) {
        return impl.step(current, Clock::now());
    }

    /**
     * Reset the PID state.
     * The current integral, previousError and output are all set to zero.
     */
    void reset() {
        impl.reset();
    }

private:
    PIDControllerImpl impl;

    template <class T>
    friend std::ostream& operator<<(std::ostream&, const PIDController<T>&);
};

template <class T>
std::ostream& operator<<(std::ostream& os, const PIDController<T>& pid) {
    os << pid.impl;
    return os;
}
