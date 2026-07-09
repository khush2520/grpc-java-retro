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

package io.grpc.auth;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.io.BaseEncoding;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Call credentials using GCP Service Account Identity JWT tokens.
 */
public final class GcpServiceAccountIdentityCallCredentials extends CallCredentials {

  private static final ScheduledExecutorService SHARED_SCHEDULER =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r, "gcp-service-account-identity-credentials-scheduler");
              t.setDaemon(true);
              return t;
            }
          });

  private final String audience;
  private final TokenFetcher tokenFetcher;
  private final Clock clock;
  private final ScheduledExecutorService scheduler;
  private final Random random = new Random();

  private final Object lock = new Object();
  private String cachedToken;
  private long modifiedExpirationTimeMillis; // exp - 30 seconds
  private boolean fetchInProgress;
  private final List<PendingApplier> pendingAppliers = new ArrayList<>();

  private long nextBackoffMs = 1000; // Initial backoff: 1 second
  private static final long MAX_BACKOFF_MS = 120_000; // Max backoff: 120 seconds
  private static final double MULTIPLIER = 1.6;
  private static final double JITTER = 0.2;

  /**
   * Constructs credentials with the given audience.
   */
  public GcpServiceAccountIdentityCallCredentials(String audience) {
    this(audience, new DefaultTokenFetcher(), new Clock(), SHARED_SCHEDULER);
  }

  @VisibleForTesting
  GcpServiceAccountIdentityCallCredentials(
      String audience,
      TokenFetcher tokenFetcher,
      Clock clock,
      ScheduledExecutorService scheduler) {
    this.audience = checkNotNull(audience, "audience");
    this.tokenFetcher = checkNotNull(tokenFetcher, "tokenFetcher");
    this.clock = checkNotNull(clock, "clock");
    this.scheduler = checkNotNull(scheduler, "scheduler");
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    long now = clock.currentTimeMillis();
    String tokenToApply = null;

    synchronized (lock) {
      if (cachedToken != null && now < modifiedExpirationTimeMillis) {
        tokenToApply = cachedToken;
        // Preemptively re-fetch 1 minute before expiration
        if (now >= modifiedExpirationTimeMillis - 60_000 && !fetchInProgress) {
          triggerFetch(appExecutor);
        }
      } else {
        pendingAppliers.add(new PendingApplier(applier, appExecutor));
        if (!fetchInProgress) {
          triggerFetch(appExecutor);
        }
      }
    }

    if (tokenToApply != null) {
      applyToken(applier, tokenToApply);
    }
  }

  private void triggerFetch(Executor executor) {
    synchronized (lock) {
      if (fetchInProgress) {
        return;
      }
      fetchInProgress = true;
    }
    executeFetch(executor);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void executeFetch(final Executor executor) {
    executor.execute(
        new Runnable() {
          @Override
          public void run() {
            try {
              String token = tokenFetcher.fetchToken(audience);
              long exp = parseExpFromToken(token);
              long expirationTimeMillis = exp * 1000;
              long modifiedExpMillis = expirationTimeMillis - 30_000;

              List<PendingApplier> appliers;
              synchronized (lock) {
                cachedToken = token;
                modifiedExpirationTimeMillis = modifiedExpMillis;
                fetchInProgress = false;
                resetBackoff();
                appliers = new ArrayList<>(pendingAppliers);
                pendingAppliers.clear();
              }

              for (final PendingApplier pending : appliers) {
                pending.executor.execute(
                    new Runnable() {
                      @Override
                      public void run() {
                        applyToken(pending.applier, token);
                      }
                    });
              }
            } catch (Exception e) {
              final Status status = mapExceptionToStatus(e);

              List<PendingApplier> appliers;
              final long delayMs;
              synchronized (lock) {
                appliers = new ArrayList<>(pendingAppliers);
                pendingAppliers.clear();
                delayMs = nextBackoffMillis();
              }

              for (final PendingApplier pending : appliers) {
                pending.executor.execute(
                    new Runnable() {
                      @Override
                      public void run() {
                        pending.applier.fail(status);
                      }
                    });
              }

              scheduler.schedule(
                  new Runnable() {
                    @Override
                    public void run() {
                      executeFetch(executor);
                    }
                  },
                  delayMs,
                  TimeUnit.MILLISECONDS);
            }
          }
        });
  }

  private void applyToken(MetadataApplier applier, String token) {
    Metadata headers = new Metadata();
    Metadata.Key<String> authKey =
        Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(authKey, "Bearer " + token);
    applier.apply(headers);
  }

  private long nextBackoffMillis() {
    long currentBackoffMs = nextBackoffMs;
    nextBackoffMs = Math.min((long) (currentBackoffMs * MULTIPLIER), MAX_BACKOFF_MS);
    double low = -JITTER * currentBackoffMs;
    double high = JITTER * currentBackoffMs;
    long randomJitter = (long) (random.nextDouble() * (high - low) + low);
    return currentBackoffMs + randomJitter;
  }

  private void resetBackoff() {
    nextBackoffMs = 1000;
  }

  private static long parseExpFromToken(String token) throws Exception {
    List<String> parts = Splitter.on('.').splitToList(token);
    if (parts.size() != 3) {
      throw new IllegalArgumentException("Invalid JWT token format");
    }
    byte[] payloadBytes = BaseEncoding.base64Url().omitPadding().decode(parts.get(1));
    String payload = new String(payloadBytes, StandardCharsets.UTF_8);
    Pattern pattern = Pattern.compile("\"exp\"\\s*:\\s*([0-9]+)");
    Matcher matcher = pattern.matcher(payload);
    if (matcher.find()) {
      return Long.parseLong(matcher.group(1));
    }
    throw new IllegalArgumentException("exp field not found in JWT payload");
  }

  private Status mapExceptionToStatus(Exception e) {
    if (e instanceof HttpError) {
      HttpError httpError = (HttpError) e;
      if (httpError.getStatusCode() == 503) {
        return Status.UNAVAILABLE
            .withDescription("Metadata server returned 503 Unavailable")
            .withCause(e);
      }
      return Status.UNAUTHENTICATED
          .withDescription("Metadata server returned HTTP status " + httpError.getStatusCode())
          .withCause(e);
    }
    if (e instanceof IOException) {
      return Status.UNAVAILABLE
          .withDescription("Failed to connect to metadata server")
          .withCause(e);
    }
    return Status.UNAUTHENTICATED
        .withDescription("Failed to parse token payload")
        .withCause(e);
  }

  @VisibleForTesting
  interface TokenFetcher {
    String fetchToken(String audience) throws IOException;
  }

  static final class HttpError extends IOException {
    private static final long serialVersionUID = 1L;
    private final int statusCode;

    HttpError(int statusCode, String message) {
      super(message);
      this.statusCode = statusCode;
    }

    int getStatusCode() {
      return statusCode;
    }
  }

  static class Clock {
    long currentTimeMillis() {
      return System.currentTimeMillis();
    }
  }

  private static final class PendingApplier {
    final MetadataApplier applier;
    final Executor executor;

    PendingApplier(MetadataApplier applier, Executor executor) {
      this.applier = applier;
      this.executor = executor;
    }
  }

  private static final class DefaultTokenFetcher implements TokenFetcher {
    private static final String METADATA_SERVER_URL =
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/"
            + "identity?audience=";

    @Override
    public String fetchToken(String audience) throws IOException {
      URL url = new URL(METADATA_SERVER_URL + URLEncoder.encode(audience, "UTF-8"));
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Metadata-Flavor", "Google");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);

      int responseCode = conn.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        try (InputStream is = conn.getInputStream();
            BufferedReader reader =
                new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
          StringBuilder response = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null) {
            response.append(line);
          }
          return response.toString().trim();
        }
      } else {
        throw new HttpError(responseCode, "Metadata server returned HTTP " + responseCode);
      }
    }
  }
}
