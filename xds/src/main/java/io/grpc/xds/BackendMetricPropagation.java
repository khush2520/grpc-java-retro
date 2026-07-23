/*
 * Copyright 2024 The gRPC Authors
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

package io.grpc.xds;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * BackendMetricPropagation represents the configuration specifying which backend metrics
 * should be propagated and reported to the LRS load reporting service.
 */
@AutoValue
public abstract class BackendMetricPropagation {

  private static final Logger logger = Logger.getLogger(BackendMetricPropagation.class.getName());

  public abstract boolean cpuUtilization();

  public abstract boolean memUtilization();

  public abstract boolean applicationUtilization();

  public abstract boolean namedMetricsAll();

  public abstract ImmutableSet<String> namedMetrics();

  public abstract boolean usePrefix();

  public static final BackendMetricPropagation LEGACY =
      create(false, false, false, true, ImmutableSet.<String>of(), false);

  public static final BackendMetricPropagation DEACTIVATED =
      create(false, false, false, false, ImmutableSet.<String>of(), true);

  public static BackendMetricPropagation create(
      boolean cpuUtilization,
      boolean memUtilization,
      boolean applicationUtilization,
      boolean namedMetricsAll,
      ImmutableSet<String> namedMetrics,
      boolean usePrefix) {
    return builder()
        .cpuUtilization(cpuUtilization)
        .memUtilization(memUtilization)
        .applicationUtilization(applicationUtilization)
        .namedMetricsAll(namedMetricsAll)
        .namedMetrics(namedMetrics)
        .usePrefix(usePrefix)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_BackendMetricPropagation.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder cpuUtilization(boolean cpuUtilization);

    public abstract Builder memUtilization(boolean memUtilization);

    public abstract Builder applicationUtilization(boolean applicationUtilization);

    public abstract Builder namedMetricsAll(boolean namedMetricsAll);

    public abstract Builder namedMetrics(ImmutableSet<String> namedMetrics);

    public abstract Builder usePrefix(boolean usePrefix);

    public abstract BackendMetricPropagation build();
  }

  /**
   * Parses the list of metric names from the CDS Cluster's lrs_report_endpoint_metrics list.
   * Unrecognized metrics or format specs are ignored with a warning log.
   */
  public static BackendMetricPropagation parse(List<String> metricNames) {
    if (metricNames == null || metricNames.isEmpty()) {
      return BackendMetricPropagation.DEACTIVATED;
    }
    boolean cpu = false;
    boolean mem = false;
    boolean app = false;
    boolean namedAll = false;
    ImmutableSet.Builder<String> namedMetricsBuilder = ImmutableSet.builder();
    for (String metricName : metricNames) {
      if (metricName == null) {
        continue;
      }
      if ("cpu_utilization".equals(metricName)) {
        cpu = true;
      } else if ("mem_utilization".equals(metricName)) {
        mem = true;
      } else if ("application_utilization".equals(metricName)) {
        app = true;
      } else if (metricName.startsWith("named_metrics.")) {
        String suffix = metricName.substring("named_metrics.".length());
        if ("*".equals(suffix)) {
          namedAll = true;
        } else if (!suffix.isEmpty()) {
          namedMetricsBuilder.add(suffix);
        } else {
          logger.log(Level.WARNING, "Ignored invalid/empty named_metrics format: " + metricName);
        }
      } else {
        logger.log(Level.WARNING, "Ignored unrecognized metric name: " + metricName);
      }
    }
    return create(cpu, mem, app, namedAll, namedMetricsBuilder.build(), true);
  }
}
