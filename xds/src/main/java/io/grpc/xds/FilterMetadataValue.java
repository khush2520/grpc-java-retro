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

import com.google.common.base.MoreObjects;
import java.util.Objects;

/** Represents a wrapper for metadata value with type URL. */
public final class FilterMetadataValue {
  private final String typeUrl;
  private final Object value;

  public FilterMetadataValue(String typeUrl, Object value) {
    this.typeUrl = typeUrl;
    this.value = value;
  }

  public String typeUrl() {
    return typeUrl;
  }

  public Object value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilterMetadataValue that = (FilterMetadataValue) o;
    return Objects.equals(typeUrl, that.typeUrl) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeUrl, value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("typeUrl", typeUrl)
        .add("value", value)
        .toString();
  }
}
