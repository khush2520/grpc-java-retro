package io.grpc.xds;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.BackoffPolicy;
import io.grpc.internal.ExponentialBackoffPolicy;
import io.grpc.internal.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public final class JwtTokenFileCallCredentials extends CallCredentials {
  private static final long REFRESH_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
  private static final long EXP_ADJUSTMENT_MILLIS = TimeUnit.SECONDS.toMillis(30);

  private final String filePath;
  private final Object lock = new Object();
  
  private String cachedToken;
  private long cacheExpirationMillis;
  
  private boolean isFetching;
  private List<PendingRequest> pendingRequests = new ArrayList<>();
  
  private ExponentialBackoffPolicy.Provider backoffPolicyProvider =
      new ExponentialBackoffPolicy.Provider();
  private BackoffPolicy backoffPolicy;
  private long backoffExpirationNanos;
  private Status lastFailureStatus;

  public interface TimeProvider {
    long currentTimeMillis();
  }

  private TimeProvider timeProvider = System::currentTimeMillis;

  public JwtTokenFileCallCredentials(String filePath) {
    this(filePath, new ExponentialBackoffPolicy.Provider());
  }

  @VisibleForTesting
  JwtTokenFileCallCredentials(String filePath, ExponentialBackoffPolicy.Provider backoffPolicyProvider) {
    this.filePath = filePath;
    this.backoffPolicyProvider = backoffPolicyProvider;
  }

  @VisibleForTesting
  void setTimeProvider(TimeProvider timeProvider) {
    this.timeProvider = timeProvider;
  }

  @Override
  public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    boolean shouldFetch = false;
    String tokenToUse = null;
    Status failureStatus = null;
    
    synchronized (lock) {
      long nowMillis = timeProvider.currentTimeMillis();
      long nowNanos = System.nanoTime();
      
      if (cachedToken == null || nowMillis >= cacheExpirationMillis) {
        if (lastFailureStatus != null && nowNanos - backoffExpirationNanos < 0) {
          failureStatus = lastFailureStatus;
        } else {
          if (!isFetching) {
            isFetching = true;
            shouldFetch = true;
          }
          pendingRequests.add(new PendingRequest(applier, appExecutor));
        }
      } else {
        tokenToUse = cachedToken;
        if (nowMillis + REFRESH_INTERVAL_MILLIS >= cacheExpirationMillis) {
          if (!isFetching) {
            isFetching = true;
            shouldFetch = true;
          }
        }
      }
    }

    if (failureStatus != null) {
      final Status fStatus = failureStatus;
      appExecutor.execute(new Runnable() {
        @Override
        public void run() {
          applier.fail(fStatus);
        }
      });
      return;
    }
    
    if (tokenToUse != null) {
      final String finalToken = tokenToUse;
      appExecutor.execute(new Runnable() {
        @Override
        public void run() {
          Metadata headers = new Metadata();
          headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + finalToken);
          applier.apply(headers);
        }
      });
    }
    
    if (shouldFetch) {
      appExecutor.execute(new Runnable() {
        @Override
        public void run() {
          fetchToken();
        }
      });
    }
  }

  private void fetchToken() {
    String token = null;
    long expMillis = 0;
    Status status = null;

    try {
      String content = new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8).trim();
      String[] parts = content.split("\\.");
      if (parts.length < 2) {
        status = Status.UNAUTHENTICATED.withDescription("Invalid JWT token format in file");
      } else {
        String payload = parts[1];
        byte[] decoded = Base64.getUrlDecoder().decode(payload);
        String json = new String(decoded, StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        Map<String, ?> map = (Map<String, ?>) JsonParser.parse(json);
        if (map.containsKey("exp")) {
          // JSON numbers might be Double or Long, casting to Number covers both.
          Number expNum = (Number) map.get("exp");
          expMillis = (long) (expNum.doubleValue() * 1000);
          token = content;
        } else {
          status = Status.UNAUTHENTICATED.withDescription("JWT token missing 'exp' claim");
        }
      }
    } catch (IOException e) {
      status = Status.UNAVAILABLE.withDescription("Failed to read JWT token file").withCause(e);
    } catch (Exception e) {
      status = Status.UNAUTHENTICATED.withDescription("Failed to parse JWT token").withCause(e);
    }

    List<PendingRequest> toNotify;
    
    synchronized (lock) {
      if (status == null) {
        cachedToken = token;
        cacheExpirationMillis = expMillis - EXP_ADJUSTMENT_MILLIS;
        backoffPolicy = null;
        backoffExpirationNanos = 0;
        lastFailureStatus = null;
      } else {
        if (backoffPolicy == null) {
          backoffPolicy = backoffPolicyProvider.get();
        }
        backoffExpirationNanos = System.nanoTime() + backoffPolicy.nextBackoffNanos();
        lastFailureStatus = status;
      }
      
      isFetching = false;
      toNotify = pendingRequests;
      pendingRequests = new ArrayList<>();
    }

    final Status finalStatus = status;
    final String finalToken = token;

    for (final PendingRequest pending : toNotify) {
      pending.executor.execute(new Runnable() {
        @Override
        public void run() {
          if (finalStatus != null) {
            pending.applier.fail(finalStatus);
          } else {
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + finalToken);
            pending.applier.apply(headers);
          }
        }
      });
    }
  }

  private static class PendingRequest {
    final MetadataApplier applier;
    final Executor executor;
    PendingRequest(MetadataApplier applier, Executor executor) {
      this.applier = applier;
      this.executor = executor;
    }
  }
}
