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
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.envoyproxy.envoy.extensions.filters.http.gcp_authn.v3.Audience;
import io.grpc.internal.GrpcUtil;
import io.grpc.xds.client.XdsResourceType.ResourceInvalidException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/** A registry for parsing metadata types. */
public final class MetadataRegistry {
  private static MetadataRegistry instance;

  private final Map<String, MetadataParser> parsers = new HashMap<>();

  private MetadataRegistry() {}

  public static synchronized MetadataRegistry getDefaultRegistry() {
    if (instance == null) {
      MetadataRegistry registry = newRegistry();
      if (GrpcUtil.getFlag("GRPC_EXPERIMENTAL_XDS_GCP_AUTHENTICATION_FILTER", false)) {
        registry.register(new GcpAuthnAudienceParser());
      }
      instance = registry;
    }
    return instance;
  }

  @VisibleForTesting
  public static synchronized void resetForTesting() {
    instance = null;
  }

  @VisibleForTesting
  public static MetadataRegistry newRegistry() {
    return new MetadataRegistry();
  }

  @VisibleForTesting
  public MetadataRegistry register(MetadataParser... newParsers) {
    for (MetadataParser parser : newParsers) {
      for (String typeUrl : parser.typeUrls()) {
        parsers.put(typeUrl, parser);
      }
    }
    return this;
  }

  @Nullable
  public MetadataParser get(String typeUrl) {
    return parsers.get(typeUrl);
  }

  public Object parse(Any any) throws ResourceInvalidException {
    MetadataParser parser = get(any.getTypeUrl());
    if (parser == null) {
      throw new ResourceInvalidException("No parser registered for type: " + any.getTypeUrl());
    }
    return parser.parse(any);
  }

  /** Parser interface for metadata types. */
  public interface MetadataParser {
    String[] typeUrls();

    Object parse(Any any) throws ResourceInvalidException;
  }

  private static final class GcpAuthnAudienceParser implements MetadataParser {
    private static final String TYPE_URL =
        "type.googleapis.com/envoy.extensions.filters.http.gcp_authn.v3.Audience";

    @Override
    public String[] typeUrls() {
      return new String[] {TYPE_URL};
    }

    @Override
    public Object parse(Any any) throws ResourceInvalidException {
      try {
        Audience audience = any.unpack(Audience.class);
        if (audience.getUrl().isEmpty()) {
          throw new ResourceInvalidException("Audience url is empty");
        }
        return audience.getUrl();
      } catch (InvalidProtocolBufferException e) {
        throw new ResourceInvalidException("Failed to unpack Audience: " + e.getMessage(), e);
      }
    }
  }
}
