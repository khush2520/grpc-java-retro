/*
 * Copyright 2021 The gRPC Authors
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

import com.google.protobuf.Message;

/**
 * Stub implementation of the GCP Authentication Filter.
 */
final class GcpAuthenticationFilter implements Filter {
  static final String TYPE_URL =
      "type.googleapis.com/envoy.extensions.filters.http.gcp_authn.v3.GcpAuthnFilterConfig";

  private GcpAuthenticationFilter() {}

  static final class Provider implements Filter.Provider {
    @Override
    public String[] typeUrls() {
      return new String[] { TYPE_URL };
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
    public ConfigOrError<? extends Filter.FilterConfig> parseFilterConfig(Message rawProtoMessage) {
      return ConfigOrError.fromError("Not implemented");
    }

    @Override
    public ConfigOrError<? extends Filter.FilterConfig> parseFilterConfigOverride(
        Message rawProtoMessage) {
      return ConfigOrError.fromError("Not implemented");
    }
  }
}
