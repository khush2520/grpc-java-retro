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

package io.grpc.xds;

import static com.google.common.truth.Truth.assertThat;

import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.AggregatedConfigSource;
import io.envoyproxy.envoy.config.core.v3.ConfigSource;
import io.envoyproxy.envoy.config.core.v3.SelfConfigSource;
import io.grpc.InsecureChannelCredentials;
import io.grpc.LoadBalancerRegistry;
import io.grpc.xds.XdsClusterResource.CdsUpdate;
import io.grpc.xds.client.BackendMetricPropagation;
import io.grpc.xds.client.Bootstrapper.ServerInfo;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link XdsClusterResource}.
 */
@RunWith(JUnit4.class)
public class XdsClusterResourceTest {

  private boolean originalFlagValue;

  @Before
  public void setUp() {
    originalFlagValue = XdsClusterResource.isEnabledOrcaLrsPropagation;
    XdsClusterResource.isEnabledOrcaLrsPropagation = true;
  }

  @After
  public void tearDown() {
    XdsClusterResource.isEnabledOrcaLrsPropagation = originalFlagValue;
  }

  @Test
  public void processCluster_parsesLrsReportEndpointMetrics() throws Exception {
    Cluster cluster = Cluster.newBuilder()
        .setName("test-cluster")
        .setType(Cluster.DiscoveryType.EDS)
        .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder()
            .setEdsConfig(ConfigSource.newBuilder()
                .setAds(AggregatedConfigSource.getDefaultInstance())))
        .setLrsServer(ConfigSource.newBuilder()
            .setSelf(SelfConfigSource.getDefaultInstance()))
        .addAllLrsReportEndpointMetrics(Arrays.asList(
            "cpu_utilization",
            "mem_utilization",
            "application_utilization",
            "named_metrics.foo",
            "named_metrics.*",
            "unrecognized_metric"
        ))
        .build();

    CdsUpdate cdsUpdate = XdsClusterResource.processCluster(
        cluster,
        /* certProviderInstances= */ Collections.emptySet(),
        /* serverInfo= */ ServerInfo.create("localhost", InsecureChannelCredentials.create()),
        LoadBalancerRegistry.getDefaultRegistry());

    BackendMetricPropagation propagation = cdsUpdate.backendMetricPropagation();
    assertThat(propagation.isOldBehavior()).isFalse();
    assertThat(propagation.cpuUtilization()).isTrue();
    assertThat(propagation.memUtilization()).isTrue();
    assertThat(propagation.applicationUtilization()).isTrue();
    assertThat(propagation.namedMetricsAll()).isTrue();
    assertThat(propagation.namedMetricKeys()).containsExactly("foo");
  }

  @Test
  public void processCluster_flagDisabled_fallsBackToOldBehavior() throws Exception {
    XdsClusterResource.isEnabledOrcaLrsPropagation = false;

    Cluster cluster = Cluster.newBuilder()
        .setName("test-cluster")
        .setType(Cluster.DiscoveryType.EDS)
        .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder()
            .setEdsConfig(ConfigSource.newBuilder()
                .setAds(AggregatedConfigSource.getDefaultInstance())))
        .setLrsServer(ConfigSource.newBuilder()
            .setSelf(SelfConfigSource.getDefaultInstance()))
        .addAllLrsReportEndpointMetrics(Arrays.asList(
            "cpu_utilization",
            "mem_utilization",
            "application_utilization",
            "named_metrics.foo",
            "named_metrics.*"
        ))
        .build();

    CdsUpdate cdsUpdate = XdsClusterResource.processCluster(
        cluster,
        /* certProviderInstances= */ Collections.emptySet(),
        /* serverInfo= */ ServerInfo.create("localhost", InsecureChannelCredentials.create()),
        LoadBalancerRegistry.getDefaultRegistry());

    BackendMetricPropagation propagation = cdsUpdate.backendMetricPropagation();
    assertThat(propagation).isEqualTo(BackendMetricPropagation.OLD_BEHAVIOR);
  }
}
