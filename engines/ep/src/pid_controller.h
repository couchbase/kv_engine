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

#include <platform/cb_time.h>
#include <chrono>
#include <functional>
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
                      cb::time::steady_clock::time_point now);

    /**
     * Step the controller - comparing now with the previous step and if dt
     * has elapsed calculate the new error and generate a new output.
     *
     * @param current The current reading of the process variable
     * @param now The time point for 'now'
     * @param checkAndMaybeReset a function that is called before the PID
     *        generates a new output. The purpose of this function is to allow
     *        the caller to examine SP, P, I, D and dt terms and maybe reset.
     * @return the PID's computed output
     */
    float step(float current,
               cb::time::steady_clock::time_point now,
               std::function<bool(PIDControllerImpl&)> checkAndMaybeReset);

    /**
     * Reset the PID state.
     * The setPoint, P, I, D and dt are all set to the given values
     * The current integral, previousError and output are all set to zero.
     */
    void reset(float setPoint,
               float kp,
               float ki,
               float kd,
               std::chrono::milliseconds dt);

    /**
     * Reset the PID state.
     * The current integral, previousError and output are all set to zero.
     */
    void reset();

    /// @return the current P term
    float getKp() const {
        return kp;
    }

    /// @return the current I term
    float getKi() const {
        return ki;
    }

    /// @return the current D term
    float getKd() const {
        return kd;
    }

    /// @return the current Dt term
    std::chrono::milliseconds getDt() const {
        return dt;
    }

    /// @return the current set point
    float getSetPoint() const {
        return setPoint;
    }

private:
    float kp{0.0};
    float ki{0.0};
    float kd{0.0};
    std::chrono::milliseconds dt{0};
    float integral{0.0};
    float setPoint{0.0};
    float previousError{0.0};
    float output{0.0};
    cb::time::steady_clock::time_point lastStep;
    friend std::ostream& operator<<(std::ostream&, const PIDControllerImpl&);
};

/**
 * PIDController wraps PIDControllerImpl providing a way to replace time for
 * test and simulation purposes
 * @tparam Clock a clock to call ::now(), allowing for replacement of time
 */
template <class Clock = cb::time::steady_clock>
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
    float step(float current,
               std::function<bool(PIDControllerImpl&)> checkAndMaybeReset) {
        return impl.step(current, Clock::now(), checkAndMaybeReset);
    }

    /**
     * Reset the PID state.
     * The current integral, previousError and output are all set to zero.
     */
    void reset(float setPoint,
               float kp,
               float ki,
               float kd,
               std::chrono::milliseconds dt) {
        impl.reset(setPoint, kp, ki, kd, dt);
    }

    /**
     * Reset the PID state.
     * The current integral, previousError and output are all set to zero.
     */
    void reset() {
        impl.reset();
    }

    bool checkAndMaybeReset(
            std::function<bool(PIDControllerImpl&)> checkAndMaybeResetFn) {
        return checkAndMaybeResetFn(impl);
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
