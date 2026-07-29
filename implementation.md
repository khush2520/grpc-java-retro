# Consolidated Implementation Plan: xDS JWT Call Credentials (gRFC A97)

## 1. Executive Summary & Goal
The goal of this implementation is to support JWT Call Credentials in `grpc-java-retro` according to gRFC A97. This includes creating a generic `JwtTokenFileCallCredentials` class in the `auth` module that asynchronously loads, validates, caches, and refreshes a JWT token from a local file, and integrating it into the xDS bootstrap config parsing (`call_creds` field under servers/authorities) and transport channel instantiation.

---

## 2. Spec Constraints Checklist

### Exact Constants
- **Environment Variable**: `GRPC_EXPERIMENTAL_XDS_BOOTSTRAP_CALL_CREDS`
- **Metadata Header Name**: `authorization`
- **Header Value Prefix**: `Bearer `
- **Refresh Margin / Expiration Skew**: 30 seconds
- **Refresh Trigger Interval**: 1 minute (60 seconds)
- **Supported Call Credential Type**: `jwt_token_file`
- **Maximum Token File Size**: 1 MB (1,048,576 bytes)

### State Conditionals
- **Token Cache States**:
  - `VALID`: `now < expirationTimeMillis`
  - `EXPIRING_SOON`: `valid` and `expirationTimeMillis - now <= 60,000` (1 minute)
  - `EXPIRED`/`UNAVAILABLE`: `now >= expirationTimeMillis` or `cachedToken == null`

### Error-Status Mappings
- **Disk Read Error / Missing File / File Too Large** -> `Status.UNAVAILABLE`
- **Invalid JWT Structure / Missing exp claim / Insecure Channel** -> `Status.UNAUTHENTICATED`
- **Attempt during backoff delay** -> Fail immediately with status of the last failed attempt (`UNAVAILABLE` or `UNAUTHENTICATED`).
- **Executor Task Rejection** -> Fail immediately with `Status.UNAVAILABLE` (or `Status.RESOURCE_EXHAUSTED`).

### Cardinality Rules
- Top-level server `call_creds` and authority server lists can contain multiple credentials.
- All supported `call_creds` in a server configuration must be compiled and combined.
- If multiple call credentials are parsed, they must be combined into a single composite credential using `CompositeCallCredentials`.
- Unsupported call credential types must be silently ignored.

---

## 3. Spec Alignment and Discrepancy Log

| No | gRFC Requirement / Aspect | Discrepancy in Plan Segments | Resolution in Final Plan |
| :--- | :--- | :--- | :--- |
| **1** | Channel Security Verification | `edge_case_plan_segment.md` omitted checking if the channel is secure in `applyRequestMetadata`. | Check `requestInfo.getSecurityLevel()` first. If it is not `SecurityLevel.PRIVACY_AND_INTEGRITY`, fail the call immediately with `Status.UNAUTHENTICATED`. |
| **2** | File Size Limit Error Mapping | `test_plan_segment.md` (REQ-9) expected `UNAUTHENTICATED` on size limit exceed, while `compatibility_plan_segment.md` expected `IOException` (which maps to `UNAVAILABLE`). | Exceeding 1 MB is treated as an I/O error (`IOException`), mapping to `Status.UNAVAILABLE`. Modified the test case name to `applyMetadata_fileTooLarge_failsUnavailable`. |
| **3** | Scheduling vs. Lazy Evaluation | `compatibility_plan_segment.md` proposed using `ScheduledExecutorService` for retry and refresh tasks. | Adopted lazy evaluation on the RPC path (as specified in `edge_case_plan_segment.md`). No active timer thread is used. Expiration of the backoff window is checked lazily when new RPCs arrive. Removed the obsolete `schedulerRunnable_weakReference` test. |
| **4** | JSON Parsing Library Choice | `edge_case_plan_segment.md` suggested using `io.grpc.internal.JsonParser` in `grpc-auth`. | `grpc-auth` cannot depend on `grpc-core` (where `JsonParser` resides). Resolved to use Gson library (adding `implementation libraries.gson` to `auth/build.gradle`) and parse with `com.google.gson.JsonParser.parseString()`. |

---

## 4. Architecture & Class Design

### New Class: `io.grpc.auth.JwtTokenFileCallCredentials`
- **Location**: `auth/src/main/java/io/grpc/auth/JwtTokenFileCallCredentials.java`
- **Extends**: `io.grpc.CallCredentials`
- **Responsibility**: Manages thread-safe lazy reading, parsing, caching, and background refreshing of a JWT token from a file.
- **Fields**:
  - `private final String filePath`: Path to the JWT file.
  - `private final Object lock = new Object()`: Lock guarding all mutable state.
  - `private String cachedToken`: Cached token string (without `"Bearer "` prefix).
  - `private long expirationTimeMillis`: Epoch ms when cached token expires (`(expSeconds - 30) * 1000`).
  - `private ReadState readState`: State of the reader (`IDLE`, `READING`, `BACKOFF`).
  - `private Status lastReadFailureStatus`: The status of the last failed attempt.
  - `private BackoffPolicy backoffPolicy`: exponential backoff manager.
  - `private long nextAttemptTimeMillis`: Timestamp after which retry is allowed.
  - `private final List<MetadataApplier> queuedAppliers`: Waiting appliers.

### Modified Class: `io.grpc.xds.client.Bootstrapper.ServerInfo`
- **Location**: `xds/src/main/java/io/grpc/xds/client/Bootstrapper.java`
- **Modifications**:
  - Add `@Nullable public abstract CallCredentials callCredentials();` to `ServerInfo`.
  - Add factory method overload:
    ```java
    public static ServerInfo create(
        String target, Object implSpecificConfig,
        boolean ignoreResourceDeletion, boolean isTrustedXdsServer,
        boolean resourceTimerIsTransientError, boolean failOnDataErrors,
        @Nullable CallCredentials callCredentials)
    ```

### Modified Class: `io.grpc.xds.client.BootstrapperImpl`
- **Location**: `xds/src/main/java/io/grpc/xds/client/BootstrapperImpl.java`
- **Modifications**:
  - Add constant `GRPC_EXPERIMENTAL_XDS_BOOTSTRAP_CALL_CREDS`.
  - Add flag `static boolean enableXdsBootstrapCallCreds`.
  - Update `parseServerInfos` to extract and parse the `"call_creds"` JSON array when the flag is true.
  - Add helper `private CallCredentials parseCallCredentials(List<Map<String, ?>> jsonList, String serverUri)`.

### Modified Class: `io.grpc.xds.GrpcXdsTransportFactory`
- **Location**: `xds/src/main/java/io/grpc/xds/GrpcXdsTransportFactory.java`
- **Modifications**:
  - In `GrpcXdsTransport` constructor, combine factory-level `callCredentials` and `serverInfo.callCredentials()` using `CompositeCallCredentials`.

### Deprecated Class: `io.grpc.xds.XdsJwtCallCredentials`
- **Location**: `xds/src/main/java/io/grpc/xds/XdsJwtCallCredentials.java`
- **Modifications**: Mark as `@Deprecated`.

---

## 5. Sequential Task Roadmap

### Task 1: Core JWT Call Credentials Implementation
- **Target Files**:
  - [auth/build.gradle](file:///usr/local/google/home/asmikhooshi/grfc-implementer/old-commits/old-commit-22/grpc-java-retro/auth/build.gradle)
  - `auth/src/main/java/io/grpc/auth/JwtTokenFileCallCredentials.java` (New File)
  - [xds/src/main/java/io/grpc/xds/XdsJwtCallCredentials.java](file:///usr/local/google/home/asmikhooshi/grfc-implementer/old-commits/old-commit-22/grpc-java-retro/xds/src/main/java/io/grpc/xds/XdsJwtCallCredentials.java)
- **Modifications**:
  - In `auth/build.gradle`, add `implementation libraries.gson`.
  - Implement `JwtTokenFileCallCredentials` extending `CallCredentials`.
  - Enforce channel security checking (`requestInfo.getSecurityLevel() == PRIVACY_AND_INTEGRITY`).
  - Read token file using a 1 MB limit.
  - Split and parse payload using Gson. Extract `exp`, compute skew-subtracted expiration timestamp.
  - Implement state transition and locking around thread-safe application and queuing.
  - Deprecate `XdsJwtCallCredentials`.
- **Verification (Unit Tests)**:
  - Add `auth/src/test/java/io/grpc/auth/JwtTokenFileCallCredentialsTest.java` verifying:
    - `applyMetadata_insecureChannel_fails`: Rejects metadata if security level is not `PRIVACY_AND_INTEGRITY`.
    - `applyMetadata_validCachedToken_cacheHit`: Synchronously applies cached token if valid.
    - `applyMetadata_tokenExpiringSoon_triggersBackgroundRefresh`: Applies cached token and triggers background read asynchronously if expires in <= 1 min.
    - `applyMetadata_concurrentCalls_queued`: Queues multiple metadata requests while load is active.
    - `applyMetadata_fileNotFound_failsUnavailable`: Maps file read failures to `Status.UNAVAILABLE`.
    - `applyMetadata_fileReadError_backoff`: Applies backoff on failure; new RPCs during backoff fail fast with the previous status.
    - `applyMetadata_malformedJwt_failsUnauthenticated`: Maps invalid segment count or decode errors to `Status.UNAUTHENTICATED`.
    - `applyMetadata_missingExpClaim_failsUnauthenticated`: Maps missing/invalid `exp` claim to `Status.UNAUTHENTICATED`.
    - `applyMetadata_fileTooLarge_failsUnavailable`: Maps file exceeding 1 MB limit to `Status.UNAVAILABLE`.
    - `applyMetadata_executorRejection_failsUnavailable`: Maps `RejectedExecutionException` from `appExecutor` to `Status.UNAVAILABLE`.

### Task 2: xDS Bootstrap Parsing and Transport Integration
- **Target Files**:
  - [xds/src/main/java/io/grpc/xds/client/Bootstrapper.java](file:///usr/local/google/home/asmikhooshi/grfc-implementer/old-commits/old-commit-22/grpc-java-retro/xds/src/main/java/io/grpc/xds/client/Bootstrapper.java)
  - [xds/src/main/java/io/grpc/xds/client/BootstrapperImpl.java](file:///usr/local/google/home/asmikhooshi/grfc-implementer/old-commits/old-commit-22/grpc-java-retro/xds/src/main/java/io/grpc/xds/client/BootstrapperImpl.java)
  - [xds/src/main/java/io/grpc/xds/GrpcXdsTransportFactory.java](file:///usr/local/google/home/asmikhooshi/grfc-implementer/old-commits/old-commit-22/grpc-java-retro/xds/src/main/java/io/grpc/xds/GrpcXdsTransportFactory.java)
- **Modifications**:
  - Add abstract `callCredentials()` field to `Bootstrapper.ServerInfo` and update creation factory methods.
  - Implement parsing of `"call_creds"` list inside `BootstrapperImpl.parseServerInfos`, compiling multiple supported credentials into a `CompositeCallCredentials`. Throw `XdsInitializationException` on config validation errors of supported types. Skip unsupported credential types.
  - Guard the parsing with `GRPC_EXPERIMENTAL_XDS_BOOTSTRAP_CALL_CREDS` environment variable.
  - Combine credentials in `GrpcXdsTransportFactory.GrpcXdsTransport` constructor using `CompositeCallCredentials`.
- **Verification (Unit Tests)**:
  - Add tests to `xds/src/test/java/io/grpc/xds/GrpcBootstrapperImplTest.java`:
    - `parseBootstrap_callCreds_flagDisabled`: Verifies `call_creds` is skipped if feature flag is false.
    - `parseBootstrap_xdsServers_jwtTokenFileCallCreds`: Verifies parsing of `jwt_token_file` config under `xds_servers` when flag is enabled.
    - `parseBootstrap_authorities_jwtTokenFileCallCreds`: Verifies parsing of `jwt_token_file` config under `authorities` when flag is enabled.
    - `parseBootstrap_unsupportedCallCredsType_ignored`: Verifies unsupported call credentials in list are ignored.
    - `parseBootstrap_malformedCallCreds_throws`: Verifies `XdsInitializationException` is thrown when config is missing mandatory parameters.
  - Add test to `xds/src/test/java/io/grpc/xds/GrpcXdsTransportFactoryTest.java`:
    - `createTransport_combinesCallCredentials`: Verifies that factory-level and server-info credentials are combined into a composite.

### Task 3: Integration Verification
- **Target Files**:
  - `xds/src/test/java/io/grpc/xds/XdsJwtCallCredsIntegrationTest.java` (New File)
- **Modifications**:
  - Implement end-to-end integration test with a fake xDS control plane.
  - Spin up fake control plane. Configure client channel with bootstrap JSON containing `jwt_token_file` call credentials.
  - Spin up server using mTLS.
  - Verify that when client calls server, the request contains the `Authorization` header with `"Bearer <jwt-token>"`.
- **Verification**:
  - Run the integration test to verify completion.

---

## 6. Compatibility, Safety, Logging, and Telemetry

### Backward Compatibility
- Features are strictly guarded by the environment variable `GRPC_EXPERIMENTAL_XDS_BOOTSTRAP_CALL_CREDS`. When not set, the bootstrap parser returns null call credentials and behaves exactly as before.
- Unsupported credentials types are skipped without throwing exceptions to ensure forward compatibility with newer configurations.
- `XdsJwtCallCredentials` is deprecated but remains in place.

### Concurrency and Locking Safety
- Access to the cached token, expiration state, queue, and backoff properties is fully guarded by `private final Object lock = new Object()`.
- Thread execution yields lock *before* triggering callbacks like `applier.apply(headers)` and `applier.fail(status)` to eliminate potential deadlock risks.
- All file reads and JSON parses are executed asynchronously on the background `appExecutor` so calling application/event-loop threads are never blocked.

### Safety Guards
- File size is restricted to 1 MB during read. If the file exceeds this limit, the task terminates and returns `IOException` to prevent memory exhaustion (DoS).
- Sensitive token strings are strictly excluded from all exceptions, log messages, and tracing metadata to prevent leakages.

### Diagnostics and Telemetry
- Logging levels:
  - `DEBUG` / `FINEST`: Logs cache hits and scheduling information.
  - `INFO`: Logs successful bootstrap parsing, background refresh successes.
  - `WARNING` / `SEVERE`: Logs disk read failures, parse failures, empty file warnings, configuration exceptions, and executor rejections.

---

## 7. Edge Case and Error Handling

### Empty or Malformed Files
- **File Not Found**: Results in `Status.UNAVAILABLE` (transient read error).
- **Empty File / Malformed JWT**: Splitting payload fails, or Base64 decoding fails. Results in `Status.UNAUTHENTICATED`.
- **Missing / Invalid exp claim**: Payload JSON is successfully parsed, but `"exp"` is missing or not a positive number. Results in `Status.UNAUTHENTICATED`.

### Backoff and Retry Details
- Failures of initial read or background refresh trigger exponential backoff using `ExponentialBackoffPolicy`.
- Backoff starts at 1 second, doubling up to 60 seconds (multiplier 2.0).
- Backoff is checked *lazily* on the RPC path:
  - During backoff, any new RPCs fail fast immediately with the status of the last failed attempt (avoiding double-reading).
  - Once backoff window expires, the next incoming RPC changes state to `READING`, queues itself, and dispatches the background read task.
- Successful read resets the backoff policy and next attempt time to zero.

### Task Rejections
- If the `appExecutor` rejects a background load task, the task rejection is caught.
- If it was a preemptive background reload, the current RPC proceeds with the cached token, resets `readState` to `IDLE`, and logs a warning.
- If it was a fresh/expired load, all queued appliers are cleared and failed with `Status.UNAVAILABLE`.
