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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.UInt64Value;
import io.envoyproxy.envoy.extensions.filters.http.gcp_authn.v3.GcpAuthnFilterConfig;
import io.envoyproxy.envoy.extensions.filters.http.gcp_authn.v3.TokenCacheConfig;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.testing.TestMethodDescriptors;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link GcpAuthenticationFilter}.
 */
@RunWith(JUnit4.class)
public class GcpAuthenticationFilterTest {

  private static final String INSTANCE_NAME = "gcp-authn-filter-instance";

  @Mock
  private Channel channel;
  @Mock
  private ClientCall<Void, Void> clientCall;
  @Mock
  private PickSubchannelArgs pickSubchannelArgs;
  @Mock
  private ScheduledExecutorService scheduler;
  @Mock
  private ClientCall.Listener<Void> listener;

  private MethodDescriptor<Void, Void> method;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    method = TestMethodDescriptors.voidMethod();
    when(channel.newCall(eq(method), any(CallOptions.class))).thenReturn(clientCall);
  }

  @Test
  public void parseFilterConfig_empty() {
    GcpAuthnFilterConfig proto = GcpAuthnFilterConfig.getDefaultInstance();
    ConfigOrError<GcpAuthenticationFilter.GcpAuthnConfig> result =
        new GcpAuthenticationFilter.Provider().parseFilterConfig(Any.pack(proto));
    assertThat(result.errorDetail).isNull();
    assertThat(result.config.getCacheSize()).isEqualTo(10);
  }

  @Test
  public void parseFilterConfig_withCacheSize() {
    GcpAuthnFilterConfig proto = GcpAuthnFilterConfig.newBuilder()
        .setCacheConfig(TokenCacheConfig.newBuilder()
            .setCacheSize(UInt64Value.newBuilder().setValue(5)))
        .build();
    ConfigOrError<GcpAuthenticationFilter.GcpAuthnConfig> result =
        new GcpAuthenticationFilter.Provider().parseFilterConfig(Any.pack(proto));
    assertThat(result.errorDetail).isNull();
    assertThat(result.config.getCacheSize()).isEqualTo(5);
  }

  @Test
  public void parseFilterConfig_invalidCacheSize() {
    GcpAuthnFilterConfig proto = GcpAuthnFilterConfig.newBuilder()
        .setCacheConfig(TokenCacheConfig.newBuilder()
            .setCacheSize(UInt64Value.newBuilder().setValue(0)))
        .build();
    ConfigOrError<GcpAuthenticationFilter.GcpAuthnConfig> result =
        new GcpAuthenticationFilter.Provider().parseFilterConfig(Any.pack(proto));
    assertThat(result.errorDetail).isNotNull();
    assertThat(result.errorDetail).contains("cacheSize must be positive");
  }

  @Test
  public void interceptCall_missingClusterSelection() {
    GcpAuthenticationFilter filter = new GcpAuthenticationFilter(INSTANCE_NAME);
    Filter.FilterConfig config = new GcpAuthenticationFilter.GcpAuthnConfig(10);
    ClientInterceptor interceptor =
        filter.buildClientInterceptor(config, null, pickSubchannelArgs, scheduler);

    CallOptions callOptions = CallOptions.DEFAULT;
    ClientCall<Void, Void> call = interceptor.interceptCall(method, callOptions, channel);

    call.start(listener, new Metadata());
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(statusCaptor.getValue().getDescription())
        .contains("Cluster name or XdsConfig is missing");
  }

  @Test
  public void interceptCall_missingCdsUpdate() {
    GcpAuthenticationFilter filter = new GcpAuthenticationFilter(INSTANCE_NAME);
    Filter.FilterConfig config = new GcpAuthenticationFilter.GcpAuthnConfig(10);
    ClientInterceptor interceptor =
        filter.buildClientInterceptor(config, null, pickSubchannelArgs, scheduler);

    XdsConfig xdsConfig = new XdsConfig(Collections.emptyMap());
    CallOptions callOptions = CallOptions.DEFAULT
        .withOption(XdsNameResolver.CLUSTER_SELECTION_KEY, "cluster-foo")
        .withOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY, xdsConfig);

    ClientCall<Void, Void> call = interceptor.interceptCall(method, callOptions, channel);

    call.start(listener, new Metadata());
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(statusCaptor.getValue().getDescription()).contains("CDS resource not found");
  }

  @Test
  public void interceptCall_noMetadataForFilterName() {
    GcpAuthenticationFilter filter = new GcpAuthenticationFilter(INSTANCE_NAME);
    Filter.FilterConfig config = new GcpAuthenticationFilter.GcpAuthnConfig(10);
    ClientInterceptor interceptor =
        filter.buildClientInterceptor(config, null, pickSubchannelArgs, scheduler);

    XdsClusterResource.CdsUpdate cdsUpdate = XdsClusterResource.CdsUpdate.forEds(
        "cluster-foo", null, null, null, null, null)
        .metadata(ImmutableMap.of())
        .roundRobinLbPolicy()
        .build();

    Map<String, XdsClusterResource.CdsUpdate> clusters = new HashMap<>();
    clusters.put("cluster-foo", cdsUpdate);
    XdsConfig xdsConfig = new XdsConfig(clusters);

    CallOptions callOptions = CallOptions.DEFAULT
        .withOption(XdsNameResolver.CLUSTER_SELECTION_KEY, "cluster-foo")
        .withOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY, xdsConfig);

    ClientCall<Void, Void> call = interceptor.interceptCall(method, callOptions, channel);
    assertThat(call).isSameInstanceAs(clientCall);
  }

  @Test
  public void interceptCall_invalidMetadataType() {
    GcpAuthenticationFilter filter = new GcpAuthenticationFilter(INSTANCE_NAME);
    Filter.FilterConfig config = new GcpAuthenticationFilter.GcpAuthnConfig(10);
    ClientInterceptor interceptor =
        filter.buildClientInterceptor(config, null, pickSubchannelArgs, scheduler);

    FilterMetadataValue metadataValue = new FilterMetadataValue("some-type", 12345);
    XdsClusterResource.CdsUpdate cdsUpdate = XdsClusterResource.CdsUpdate.forEds(
        "cluster-foo", null, null, null, null, null)
        .metadata(ImmutableMap.of(INSTANCE_NAME, metadataValue))
        .roundRobinLbPolicy()
        .build();

    Map<String, XdsClusterResource.CdsUpdate> clusters = new HashMap<>();
    clusters.put("cluster-foo", cdsUpdate);
    XdsConfig xdsConfig = new XdsConfig(clusters);

    CallOptions callOptions = CallOptions.DEFAULT
        .withOption(XdsNameResolver.CLUSTER_SELECTION_KEY, "cluster-foo")
        .withOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY, xdsConfig);

    ClientCall<Void, Void> call = interceptor.interceptCall(method, callOptions, channel);

    call.start(listener, new Metadata());
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(statusCaptor.getValue().getDescription()).contains("Metadata value is not a String");
  }

  @Test
  public void interceptCall_successfulAudienceExtraction() {
    GcpAuthenticationFilter filter = new GcpAuthenticationFilter(INSTANCE_NAME);
    Filter.FilterConfig config = new GcpAuthenticationFilter.GcpAuthnConfig(10);
    ClientInterceptor interceptor =
        filter.buildClientInterceptor(config, null, pickSubchannelArgs, scheduler);

    FilterMetadataValue metadataValue = new FilterMetadataValue(
        "type.googleapis.com/envoy.extensions.filters.http.gcp_authn.v3.Audience",
        "https://example.com/audience");
    XdsClusterResource.CdsUpdate cdsUpdate = XdsClusterResource.CdsUpdate.forEds(
        "cluster-foo", null, null, null, null, null)
        .metadata(ImmutableMap.of(INSTANCE_NAME, metadataValue))
        .roundRobinLbPolicy()
        .build();

    Map<String, XdsClusterResource.CdsUpdate> clusters = new HashMap<>();
    clusters.put("cluster-foo", cdsUpdate);
    XdsConfig xdsConfig = new XdsConfig(clusters);

    CallOptions callOptions = CallOptions.DEFAULT
        .withOption(XdsNameResolver.CLUSTER_SELECTION_KEY, "cluster-foo")
        .withOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY, xdsConfig);

    ClientCall<Void, Void> call = interceptor.interceptCall(method, callOptions, channel);
    assertThat(call).isSameInstanceAs(clientCall);

    ArgumentCaptor<CallOptions> optionsCaptor = ArgumentCaptor.forClass(CallOptions.class);
    verify(channel).newCall(eq(method), optionsCaptor.capture());
    CallOptions capturedOptions = optionsCaptor.getValue();
    assertThat(capturedOptions.getCredentials()).isNotNull();

    // Verify cache has cached the credentials
    Map<String, CallCredentials> cache = filter.getCredentialsCache();
    assertThat(cache).containsKey("https://example.com/audience");
    assertThat(capturedOptions.getCredentials()).isSameInstanceAs(cache.get("https://example.com/audience"));

    // Call again and verify same credentials instance is reused
    interceptor.interceptCall(method, callOptions, channel);
    assertThat(filter.getCredentialsCache().size()).isEqualTo(1);
  }

  @Test
  public void credentialsCache_lruEviction() {
    GcpAuthenticationFilter filter = new GcpAuthenticationFilter(INSTANCE_NAME);
    Filter.FilterConfig config = new GcpAuthenticationFilter.GcpAuthnConfig(2); // limit cache to 2
    ClientInterceptor interceptor =
        filter.buildClientInterceptor(config, null, pickSubchannelArgs, scheduler);

    XdsConfig xdsConfig = createXdsConfigWithAudiences("audience1", "audience2", "audience3");

    // Retrieve audience1
    CallOptions callOptions1 = CallOptions.DEFAULT
        .withOption(XdsNameResolver.CLUSTER_SELECTION_KEY, "cluster-audience1")
        .withOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY, xdsConfig);
    interceptor.interceptCall(method, callOptions1, channel);

    // Retrieve audience2
    CallOptions callOptions2 = CallOptions.DEFAULT
        .withOption(XdsNameResolver.CLUSTER_SELECTION_KEY, "cluster-audience2")
        .withOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY, xdsConfig);
    interceptor.interceptCall(method, callOptions2, channel);

    Map<String, CallCredentials> cache = filter.getCredentialsCache();
    assertThat(cache.size()).isEqualTo(2);
    assertThat(cache).containsKey("audience1");
    assertThat(cache).containsKey("audience2");

    // Retrieve audience3
    CallOptions callOptions3 = CallOptions.DEFAULT
        .withOption(XdsNameResolver.CLUSTER_SELECTION_KEY, "cluster-audience3")
        .withOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY, xdsConfig);
    interceptor.interceptCall(method, callOptions3, channel);

    assertThat(cache.size()).isEqualTo(2);
    assertThat(cache).doesNotContainKey("audience1"); // oldest evicted
    assertThat(cache).containsKey("audience2");
    assertThat(cache).containsKey("audience3");
  }

  @Test
  public void credentialsCache_closeClearsCache() {
    GcpAuthenticationFilter filter = new GcpAuthenticationFilter(INSTANCE_NAME);
    Filter.FilterConfig config = new GcpAuthenticationFilter.GcpAuthnConfig(5);
    ClientInterceptor interceptor =
        filter.buildClientInterceptor(config, null, pickSubchannelArgs, scheduler);

    XdsConfig xdsConfig = createXdsConfigWithAudiences("audience1");
    CallOptions callOptions = CallOptions.DEFAULT
        .withOption(XdsNameResolver.CLUSTER_SELECTION_KEY, "cluster-audience1")
        .withOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY, xdsConfig);
    interceptor.interceptCall(method, callOptions, channel);

    assertThat(filter.getCredentialsCache().size()).isEqualTo(1);
    filter.close();
    assertThat(filter.getCredentialsCache()).isEmpty();
  }

  private XdsConfig createXdsConfigWithAudiences(String... audiences) {
    Map<String, XdsClusterResource.CdsUpdate> clusters = new HashMap<>();
    for (String aud : audiences) {
      FilterMetadataValue metadataValue = new FilterMetadataValue(
          "type.googleapis.com/envoy.extensions.filters.http.gcp_authn.v3.Audience",
          aud);
      XdsClusterResource.CdsUpdate cdsUpdate = XdsClusterResource.CdsUpdate.forEds(
          "cluster-" + aud, null, null, null, null, null)
          .metadata(ImmutableMap.of(INSTANCE_NAME, metadataValue))
          .roundRobinLbPolicy()
          .build();
      clusters.put("cluster-" + aud, cdsUpdate);
    }
    return new XdsConfig(clusters);
  }
}
