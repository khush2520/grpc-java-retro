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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.BaseEncoding;
import io.grpc.CallCredentials.MetadataApplier;
import io.grpc.CallCredentials.RequestInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for {@link GcpServiceAccountIdentityCallCredentials}.
 */
@RunWith(JUnit4.class)
public final class GcpServiceAccountIdentityCallCredentialsTest {

  @Rule
  public final MockitoRule mocks = MockitoJUnit.rule();

  private static final String AUDIENCE = "https://example.com/audience";
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  @Mock
  private GcpServiceAccountIdentityCallCredentials.TokenFetcher tokenFetcher;
  @Mock
  private ScheduledExecutorService scheduler;
  @Mock
  private MetadataApplier applier1;
  @Mock
  private MetadataApplier applier2;

  private final FakeClock clock = new FakeClock();
  private final List<Runnable> executorTasks = new ArrayList<>();
  private final List<Runnable> scheduledTasks = new ArrayList<>();
  private final List<Long> scheduledDelays = new ArrayList<>();

  private final Executor directExecutor =
      new Executor() {
        @Override
        public void execute(Runnable r) {
          executorTasks.add(r);
        }
      };

  private GcpServiceAccountIdentityCallCredentials credentials;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    credentials =
        new GcpServiceAccountIdentityCallCredentials(AUDIENCE, tokenFetcher, clock, scheduler);

    when(scheduler.schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class)))
        .thenAnswer(
            new Answer<ScheduledFuture<?>>() {
              @Override
              public ScheduledFuture<?> answer(InvocationOnMock invocation) {
                scheduledTasks.add(invocation.getArgument(0));
                scheduledDelays.add(invocation.getArgument(1));
                return mock(ScheduledFuture.class);
              }
            });
  }

  private void runExecutorTasks() {
    List<Runnable> tasks = new ArrayList<>(executorTasks);
    executorTasks.clear();
    for (Runnable task : tasks) {
      task.run();
    }
  }

  private String createJwtToken(long expSeconds) {
    String header = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjEifQ"; // {"alg":"RS256","kid":"1"}
    String payloadJson = "{\"aud\":\"" + AUDIENCE + "\",\"exp\":" + expSeconds + "}";
    String payload =
        BaseEncoding.base64Url()
            .omitPadding()
            .encode(payloadJson.getBytes(StandardCharsets.UTF_8));
    String signature = "fake-signature";
    return header + "." + payload + "." + signature;
  }

  @Test
  public void testSuccessfulTokenFetch() throws Exception {
    long exp = clock.currentTimeMillis() / 1000 + 3600; // expires in 1 hour
    String token = createJwtToken(exp);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenReturn(token);

    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);

    // Initial call should queue the fetch task in the executor
    assertEquals(1, executorTasks.size());
    verify(tokenFetcher, never()).fetchToken(any(String.class));

    // Run the fetch task
    runExecutorTasks();

    // Verify token was fetched
    verify(tokenFetcher, times(1)).fetchToken(AUDIENCE);

    // Verify application callback is scheduled on executor
    assertEquals(1, executorTasks.size());

    // Run application callback
    runExecutorTasks();

    ArgumentCaptor<Metadata> headersCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(applier1).apply(headersCaptor.capture());
    Metadata headers = headersCaptor.getValue();
    assertEquals("Bearer " + token, headers.get(AUTHORIZATION));
  }

  @Test
  public void testTokenCaching() throws Exception {
    long exp = clock.currentTimeMillis() / 1000 + 3600;
    String token = createJwtToken(exp);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenReturn(token);

    // First call triggers fetch
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);
    runExecutorTasks(); // Run fetch task
    runExecutorTasks(); // Run applier task
    verify(applier1).apply(any(Metadata.class));

    // Second call should use cached token immediately (synchronously) without scheduling
    // executor tasks
    assertTrue(executorTasks.isEmpty());
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier2);

    ArgumentCaptor<Metadata> headersCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(applier2).apply(headersCaptor.capture());
    Metadata headers = headersCaptor.getValue();
    assertEquals("Bearer " + token, headers.get(AUTHORIZATION));

    // Token was only fetched once
    verify(tokenFetcher, times(1)).fetchToken(AUDIENCE);
  }

  @Test
  public void testTokenExpiration() throws Exception {
    long exp = clock.currentTimeMillis() / 1000 + 3600; // expires in 1 hour
    String token1 = createJwtToken(exp);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenReturn(token1);

    // First call triggers fetch
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);
    runExecutorTasks();
    runExecutorTasks();

    // Advance clock past expiration (exp - 30 seconds = 3570 seconds)
    // Advance clock by 3580 seconds
    clock.timeMillis += 3580 * 1000L;

    long newExp = clock.currentTimeMillis() / 1000 + 3600;
    String token2 = createJwtToken(newExp);
    Mockito.reset(tokenFetcher);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenReturn(token2);

    // Second call should trigger a new fetch because the first one is expired
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier2);
    assertEquals(1, executorTasks.size()); // New fetch is queued
    runExecutorTasks();
    runExecutorTasks();

    ArgumentCaptor<Metadata> headersCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(applier2).apply(headersCaptor.capture());
    assertEquals("Bearer " + token2, headersCaptor.getValue().get(AUTHORIZATION));
    verify(tokenFetcher, times(1)).fetchToken(AUDIENCE);
  }

  @Test
  public void testPreemptiveRefresh() throws Exception {
    long exp = clock.currentTimeMillis() / 1000 + 3600; // 3600 seconds
    String token1 = createJwtToken(exp);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenReturn(token1);

    // Initial fetch
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);
    runExecutorTasks();
    runExecutorTasks();

    // Modified expiration time is exp - 30s = 3570s.
    // Preemptive refresh window starts at exp - 30s - 60s = 3510s.
    // Advance clock to 3520 seconds (inside refresh window, but not expired).
    clock.timeMillis += 3520 * 1000L;

    long newExp = clock.currentTimeMillis() / 1000 + 3600;
    String token2 = createJwtToken(newExp);
    Mockito.reset(tokenFetcher);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenReturn(token2);

    // Call applyRequestMetadata. It should apply token1 immediately and trigger fetch in
    // background.
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier2);

    // Verify token1 is applied immediately
    ArgumentCaptor<Metadata> headersCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(applier2).apply(headersCaptor.capture());
    assertEquals("Bearer " + token1, headersCaptor.getValue().get(AUTHORIZATION));

    // Verify background fetch was scheduled
    assertEquals(1, executorTasks.size());
    runExecutorTasks(); // Run fetch task
    verify(tokenFetcher, times(1)).fetchToken(AUDIENCE);
  }

  @Test
  public void testQueueing() throws Exception {
    long exp = clock.currentTimeMillis() / 1000 + 3600;
    String token = createJwtToken(exp);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenReturn(token);

    // Trigger initial fetch
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);
    // Call again before running executor tasks (fetch in progress)
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier2);

    // Verify only one fetch is scheduled
    assertEquals(1, executorTasks.size());

    // Run fetch
    runExecutorTasks();
    verify(tokenFetcher, times(1)).fetchToken(AUDIENCE);

    // Verify both appliers are scheduled
    assertEquals(2, executorTasks.size());

    runExecutorTasks();
    verify(applier1).apply(any(Metadata.class));
    verify(applier2).apply(any(Metadata.class));
  }

  @Test
  public void testFetchFailure_Unavailable() throws Exception {
    when(tokenFetcher.fetchToken(AUDIENCE)).thenThrow(new IOException("Connection timed out"));

    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);
    runExecutorTasks(); // Run fetch task

    // Verify failure callback is queued
    assertEquals(1, executorTasks.size());
    runExecutorTasks();

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(applier1).fail(statusCaptor.capture());
    assertEquals(Status.Code.UNAVAILABLE, statusCaptor.getValue().getCode());

    // Verify retry is scheduled
    assertEquals(1, scheduledTasks.size());
    long delay = scheduledDelays.get(0);
    assertTrue(delay >= 800 && delay <= 1200); // 1000ms +- 200ms jitter
  }

  @Test
  public void testFetchFailure_Unauthenticated() throws Exception {
    when(tokenFetcher.fetchToken(AUDIENCE))
        .thenThrow(new GcpServiceAccountIdentityCallCredentials.HttpError(403, "Forbidden"));

    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);
    runExecutorTasks();
    runExecutorTasks();

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(applier1).fail(statusCaptor.capture());
    assertEquals(Status.Code.UNAUTHENTICATED, statusCaptor.getValue().getCode());
  }

  @Test
  public void testBackoffRetry() throws Exception {
    when(tokenFetcher.fetchToken(AUDIENCE)).thenThrow(new IOException("Network error"));

    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);
    runExecutorTasks(); // First fetch fails
    runExecutorTasks(); // First applier fails

    assertEquals(1, scheduledTasks.size());
    long firstDelay = scheduledDelays.get(0);
    // first delay is 1000ms +- 200ms jitter (800ms to 1200ms)
    assertTrue(firstDelay >= 800 && firstDelay <= 1200);

    // Run first retry task
    Runnable retryTask1 = scheduledTasks.get(0);
    scheduledTasks.clear();
    scheduledDelays.clear();

    retryTask1.run(); // Triggers second executeFetch
    runExecutorTasks(); // Second fetch fails

    assertEquals(1, scheduledTasks.size());
    long secondDelay = scheduledDelays.get(0);
    // second base delay is 1600ms, +- 320ms jitter (1280ms to 1920ms)
    assertTrue(secondDelay >= 1280 && secondDelay <= 1920);

    // Now make the next fetch succeed
    long exp = clock.currentTimeMillis() / 1000 + 3600;
    String token = createJwtToken(exp);
    Mockito.reset(tokenFetcher);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenReturn(token);

    // Queue another applier
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier2);

    // Run second retry task
    Runnable retryTask2 = scheduledTasks.get(0);
    scheduledTasks.clear();
    scheduledDelays.clear();

    retryTask2.run();
    runExecutorTasks(); // Fetch succeeds
    runExecutorTasks(); // Applier succeeds

    verify(applier2).apply(any(Metadata.class));

    // Next failure should reset backoff to initial 1s
    Mockito.reset(tokenFetcher);
    when(tokenFetcher.fetchToken(AUDIENCE)).thenThrow(new IOException("Another network error"));

    // Advance clock past expiration
    clock.timeMillis += 3580 * 1000L;
    credentials.applyRequestMetadata(mock(RequestInfo.class), directExecutor, applier1);
    runExecutorTasks();
    runExecutorTasks();

    assertEquals(1, scheduledTasks.size());
    long thirdDelay = scheduledDelays.get(0);
    assertTrue(thirdDelay >= 800 && thirdDelay <= 1200);
  }

  private static class FakeClock extends GcpServiceAccountIdentityCallCredentials.Clock {
    long timeMillis = 1000000L;

    @Override
    long currentTimeMillis() {
      return timeMillis;
    }
  }
}
