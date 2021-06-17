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

#include "pid_controller.h"
#include "bucket_logger.h"

PIDControllerImpl::PIDControllerImpl(
        float setPoint,
        float kp,
        float ki,
        float kd,
        std::chrono::milliseconds dt,
        std::chrono::time_point<std::chrono::steady_clock> now)
    : kp{kp}, ki{ki}, kd{kd}, dt{dt}, setPoint(setPoint), lastStep(now) {
}

float PIDControllerImpl::step(
        float current,
        std::chrono::time_point<std::chrono::steady_clock> now,
        std::function<bool(PIDControllerImpl&)> checkAndMaybeReset) {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - lastStep);

    // Stabilise the step to an interval
    // http://brettbeauregard.com/blog/2011/04/improving-the-beginner%e2%80%99s-pid-sample-time/
    if (duration < dt) {
        return output;
    }

    // Invoke this function now which is intended to allow the PID owner to
    // reset the PID if configuration has changed.
    (void)checkAndMaybeReset(*this);

    float error = setPoint - current;

    integral += (error * duration.count());

    float derivative = (error - previousError) / duration.count();

    output = (kp * error) + (ki * integral) + (kd * derivative);

    previousError = error;
    lastStep = now;
    return output;
}

void PIDControllerImpl::reset(float setPoint,
                              float kp,
                              float ki,
                              float kd,
                              std::chrono::milliseconds dt) {
    this->setPoint = setPoint;
    this->kp = kp;
    this->ki = ki;
    this->kd = kd;
    this->dt = dt;
    reset();
}

void PIDControllerImpl::reset() {
    integral = 0;
    previousError = 0;
    output = 0;
}

std::ostream& operator<<(std::ostream& os, const PIDControllerImpl& pid) {
    os << "kp:" << pid.kp << ", ki:" << pid.ki << ", kd:" << pid.kd
       << ", sp:" << pid.setPoint << ", integral:" << pid.integral
       << ", dt:" << pid.dt.count()
       << ", lastStep:" << pid.lastStep.time_since_epoch().count()
       << ", output:" << pid.output;
    return os;
}
