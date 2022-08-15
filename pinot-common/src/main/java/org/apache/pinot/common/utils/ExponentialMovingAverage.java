/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.common.utils;

import com.google.common.base.Preconditions;
import java.util.Timer;
import java.util.TimerTask;


public class ExponentialMovingAverage {
  private final double _alpha;
  private final long _autoDecayWindowMs;

  private final int _warmUpCount;
  private int _currentCount;

  private volatile double _average;
  private volatile long _lastUpdatedTimeMs;

  /**
   * Constructor
   *
   * @param alpha Determines how much weightage should be given to the new value.
   * @param autoDecayWindowMs Time interval to periodically decay the average if no updates are received. For
   *                          example if autoDecayWindowMs = 30s, if average is not updated for a period of 30
   *                          seconds, we automatically update the average to 0.0 with a weightage of alpha.
   * @param warmUpCount The number of incoming values to ignore before starting the average calculation.
   * @param avgInitializationVal The default value to initialize for average.
   */
  public ExponentialMovingAverage(double alpha, long autoDecayWindowMs, int warmUpCount,
      double avgInitializationVal) {
    _alpha = alpha;
    _autoDecayWindowMs = autoDecayWindowMs;
    _warmUpCount = warmUpCount;

    _currentCount = 0;
    _lastUpdatedTimeMs = 0;
    _average = avgInitializationVal;

    if (_autoDecayWindowMs > 0) {
      // Create a timer to automatically decay the average if updates are not performed in the last _autoDecayWindowMs.
      Timer timer = new Timer();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          if (_lastUpdatedTimeMs > 0 && (System.currentTimeMillis() - _lastUpdatedTimeMs) > _autoDecayWindowMs) {
            compute(0.0);
          }
        }
      }, 0, _autoDecayWindowMs);
    }
  }

  /**
   * Returns the exponentially weighted moving average.
   */
  public double getAverage() {
    return _average;
  }

  /**
   * Adds a value to the exponentially weighted moving average. If warmUpCount is not reached yet, this value is
   * ignored.
   * @param value
   * @return the updated exponentially weighted moving average.
   */
  public synchronized double compute(double value) {
    Preconditions.checkState(value >= 0.0, "Latency value is negative " + value);
    _lastUpdatedTimeMs = System.currentTimeMillis();

    ++_currentCount;
    if (_currentCount < _warmUpCount) {
      return _average;
    }

    _average = value * _alpha + _average * (1 - _alpha);
    return _average;
  }
}
