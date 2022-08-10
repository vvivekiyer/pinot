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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(Vivek): Add warm-up parameter. Based on this parameters, the calculator should skip the first few values.
//  This is needed because the first few queries on the server incur high latencies because of paging.
public class ExponentialMovingAverage {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExponentialMovingAverage.class);

  private final double _alpha;
  private final long _autoDecayWindowMs;

  private final int _warmUpCount;
  private int _currentCount;

  private volatile double _average;
  private volatile long _lastUpdateMs;

  public ExponentialMovingAverage(double alpha, long autoDecayWindowMs, int warmUpCount) {
    _alpha = alpha;
    _autoDecayWindowMs = autoDecayWindowMs;

    _warmUpCount = warmUpCount;
    _currentCount = 0;

    _average = 0.0;
    _lastUpdateMs = 0;

    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        if (_lastUpdateMs > 0 && (System.currentTimeMillis() - _lastUpdateMs) > _autoDecayWindowMs) {
          compute(0.0);
        }
      }
    }, 0, _autoDecayWindowMs);
  }

  public double getAverage() {
    return _average;
  }

  public synchronized double compute(double value) {
    Preconditions.checkState(value >= 0.0, "Latency value is negative " + Double.toString(value));
    _lastUpdateMs = System.currentTimeMillis();

    ++_currentCount;
    if (_currentCount < _warmUpCount) {
      return _average;
    }

    _average = value * _alpha + _average * (1 - _alpha);
    return _average;
  }
}
