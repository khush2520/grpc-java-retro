/*
 * Copyright 2026 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds.client;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import java.util.List;

/**
 * Parsed propagation rules for backend metrics.
 */
@AutoValue
public abstract class BackendMetricPropagation {
  public abstract boolean cpuUtilization();

  public abstract boolean memUtilization();

  public abstract boolean applicationUtilization();

  public abstract boolean namedMetricsAll();

  public abstract ImmutableSet<String> namedMetricKeys();

  public abstract boolean isOldBehavior();

  public boolean shouldPropagateNamedMetric(String key) {
    if (isOldBehavior()) {
      return true;
    }
    return namedMetricsAll() || namedMetricKeys().contains(key);
  }

  public static BackendMetricPropagation create(
      boolean cpuUtilization, boolean memUtilization, boolean applicationUtilization,
      boolean namedMetricsAll, ImmutableSet<String> namedMetricKeys, boolean isOldBehavior) {
    return new AutoValue_BackendMetricPropagation(
        cpuUtilization, memUtilization, applicationUtilization,
        namedMetricsAll, namedMetricKeys, isOldBehavior);
  }

  public static final BackendMetricPropagation OLD_BEHAVIOR =
      new AutoValue_BackendMetricPropagation(
          /* cpuUtilization= */ false,
          /* memUtilization= */ false,
          /* applicationUtilization= */ false,
          /* namedMetricsAll= */ false,
          /* namedMetricKeys= */ ImmutableSet.of(),
          /* isOldBehavior= */ true);

  public static final BackendMetricPropagation PROPAGATE_NONE =
      new AutoValue_BackendMetricPropagation(
          /* cpuUtilization= */ false,
          /* memUtilization= */ false,
          /* applicationUtilization= */ false,
          /* namedMetricsAll= */ false,
          /* namedMetricKeys= */ ImmutableSet.of(),
          /* isOldBehavior= */ false);

  /**
   * Parses standard and custom metrics from the CDS resource configuration.
   */
  public static BackendMetricPropagation fromCds(List<String> metrics) {
    boolean cpu = false;
    boolean mem = false;
    boolean app = false;
    boolean allNamed = false;
    ImmutableSet.Builder<String> namedKeys = ImmutableSet.builder();

    for (String metric : metrics) {
      if ("cpu_utilization".equals(metric)) {
        cpu = true;
      } else if ("mem_utilization".equals(metric)) {
        mem = true;
      } else if ("application_utilization".equals(metric)) {
        app = true;
      } else if ("named_metrics.*".equals(metric)) {
        allNamed = true;
      } else if (metric.startsWith("named_metrics.")) {
        String key = metric.substring("named_metrics.".length());
        if (!key.isEmpty()) {
          namedKeys.add(key);
        }
      }
    }

    return new AutoValue_BackendMetricPropagation(
        cpu, mem, app, allNamed, namedKeys.build(), /* isOldBehavior= */ false);
  }
}
