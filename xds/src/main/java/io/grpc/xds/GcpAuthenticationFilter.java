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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.extensions.filters.http.gcp_authn.v3.GcpAuthnFilterConfig;
import io.envoyproxy.envoy.extensions.filters.http.gcp_authn.v3.TokenCacheConfig;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.CompositeCallCredentials;
import io.grpc.Context;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.xds.Filter.ClientInterceptorBuilder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;

/**
 * Filter implementation for GCP Authentication.
 */
final class GcpAuthenticationFilter implements Filter, ClientInterceptorBuilder {

  private final String filterInstanceName;
  private final Object lock = new Object();
  private Map<String, CallCredentials> credentialsCache;

  GcpAuthenticationFilter() {
    this("");
  }

  GcpAuthenticationFilter(String filterInstanceName) {
    this.filterInstanceName = filterInstanceName;
  }

  @VisibleForTesting
  Map<String, CallCredentials> getCredentialsCache() {
    synchronized (lock) {
      return credentialsCache;
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      if (credentialsCache != null) {
        credentialsCache.clear();
      }
    }
  }

  @Nullable
  @Override
  public ClientInterceptor buildClientInterceptor(
      FilterConfig config,
      @Nullable FilterConfig overrideConfig,
      PickSubchannelArgs args,
      ScheduledExecutorService scheduler) {
    GcpAuthnConfig authnConfig = (GcpAuthnConfig) config;
    if (overrideConfig != null) {
      authnConfig = (GcpAuthnConfig) overrideConfig;
    }
    final int cacheSize = authnConfig.getCacheSize();
    synchronized (lock) {
      if (credentialsCache == null) {
        credentialsCache = new LinkedHashMap<String, CallCredentials>(cacheSize, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<String, CallCredentials> eldest) {
            return size() > cacheSize;
          }
        };
      }
    }

    return new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        String clusterName = callOptions.getOption(XdsNameResolver.CLUSTER_SELECTION_KEY);
        XdsConfig xdsConfig = callOptions.getOption(XdsNameResolver.XDS_CONFIG_CALL_OPTION_KEY);
        if (clusterName == null || xdsConfig == null) {
          return new FailingClientCall<>(
              Status.UNAVAILABLE.withDescription(
                  "Cluster name or XdsConfig is missing in CallOptions"),
              getExecutor(callOptions));
        }
        XdsClusterResource.CdsUpdate cdsUpdate = xdsConfig.getClusters().get(clusterName);
        if (cdsUpdate == null) {
          return new FailingClientCall<>(
              Status.UNAVAILABLE.withDescription(
                  "CDS resource not found for cluster: " + clusterName),
              getExecutor(callOptions));
        }
        FilterMetadataValue metadataValue = cdsUpdate.metadata().get(filterInstanceName);
        if (metadataValue == null) {
          return next.newCall(method, callOptions);
        }
        Object value = metadataValue.value();
        if (!(value instanceof String)) {
          return new FailingClientCall<>(
              Status.UNAVAILABLE.withDescription(
                  "Metadata value is not a String for filter: " + filterInstanceName),
              getExecutor(callOptions));
        }
        String audience = (String) value;
        CallCredentials credentials;
        synchronized (lock) {
          credentials = credentialsCache.get(audience);
          if (credentials == null) {
            credentials = new io.grpc.auth.GcpServiceAccountIdentityCallCredentials(audience);
            credentialsCache.put(audience, credentials);
          }
        }
        CallCredentials existingCredentials = callOptions.getCredentials();
        if (existingCredentials != null) {
          credentials = new CompositeCallCredentials(existingCredentials, credentials);
        }
        return next.newCall(method, callOptions.withCallCredentials(credentials));
      }
    };
  }

  private static Executor getExecutor(CallOptions callOptions) {
    Executor executor = callOptions.getExecutor();
    return executor != null ? executor : MoreExecutors.directExecutor();
  }

  static final class GcpAuthnConfig implements FilterConfig {
    private final int cacheSize;

    GcpAuthnConfig(int cacheSize) {
      this.cacheSize = cacheSize;
    }

    @Override
    public String typeUrl() {
      return Provider.TYPE_URL;
    }

    public int getCacheSize() {
      return cacheSize;
    }
  }

  static final class Provider implements Filter.Provider {
    static final String TYPE_URL =
        "type.googleapis.com/envoy.extensions.filters.http.gcp_authn.v3.GcpAuthnFilterConfig";

    @Override
    public String[] typeUrls() {
      return new String[] {TYPE_URL};
    }

    @Override
    public boolean isClientFilter() {
      return true;
    }

    @Override
    public boolean isServerFilter() {
      return false;
    }

    @Override
    public Filter newInstance() {
      return new GcpAuthenticationFilter();
    }

    @Override
    public Filter newInstance(String name) {
      return new GcpAuthenticationFilter(name);
    }

    @Override
    public ConfigOrError<GcpAuthnConfig> parseFilterConfig(Message rawProtoMessage) {
      if (!(rawProtoMessage instanceof Any)) {
        return ConfigOrError.fromError("Invalid config type: " + rawProtoMessage.getClass());
      }
      Any anyMessage = (Any) rawProtoMessage;
      GcpAuthnFilterConfig gcpAuthnFilterConfig;
      try {
        gcpAuthnFilterConfig = anyMessage.unpack(GcpAuthnFilterConfig.class);
      } catch (InvalidProtocolBufferException e) {
        return ConfigOrError.fromError("Invalid proto: " + e);
      }

      int cacheSize = 10;
      if (gcpAuthnFilterConfig.hasCacheConfig()) {
        TokenCacheConfig cacheConfig = gcpAuthnFilterConfig.getCacheConfig();
        if (cacheConfig.hasCacheSize()) {
          long val = cacheConfig.getCacheSize().getValue();
          if (val <= 0) {
            return ConfigOrError.fromError("cacheSize must be positive: " + val);
          }
          cacheSize = (int) val;
        }
      }
      return ConfigOrError.fromConfig(new GcpAuthnConfig(cacheSize));
    }

    @Override
    public ConfigOrError<GcpAuthnConfig> parseFilterConfigOverride(Message rawProtoMessage) {
      return parseFilterConfig(rawProtoMessage);
    }
  }

  private static final class FailingClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
    private final Status error;
    private final Executor callExecutor;
    private final Context context;

    FailingClientCall(Status error, Executor callExecutor) {
      this.error = error;
      this.callExecutor = callExecutor;
      this.context = Context.current();
    }

    @Override
    public void start(final ClientCall.Listener<RespT> listener, Metadata headers) {
      callExecutor.execute(
          new Runnable() {
            @Override
            public void run() {
              Context previous = context.attach();
              try {
                listener.onClose(error, new Metadata());
              } finally {
                context.detach(previous);
              }
            }
          });
    }

    @Override
    public void request(int numMessages) {}

    @Override
    public void cancel(String message, Throwable cause) {}

    @Override
    public void halfClose() {}

    @Override
    public void sendMessage(ReqT message) {}
  }
}
