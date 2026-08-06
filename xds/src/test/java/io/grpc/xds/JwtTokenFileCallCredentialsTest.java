package io.grpc.xds;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.JsonParser;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class JwtTokenFileCallCredentialsTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private JwtTokenFileCallCredentials creds;
  private File tokenFile;

  @Before
  public void setUp() throws Exception {
    tokenFile = tempFolder.newFile("token.jwt");
    creds = new JwtTokenFileCallCredentials(tokenFile.getAbsolutePath());
    creds.setTimeProvider(new JwtTokenFileCallCredentials.TimeProvider() {
      @Override
      public long currentTimeMillis() {
        return 0L; // Start at epoch
      }
    });
  }

  private void writeToken(long expSeconds) throws Exception {
    String payload = String.format("{\"exp\":%d}", expSeconds);
    String b64Payload = Base64.getUrlEncoder().withoutPadding().encodeToString(
        payload.getBytes(StandardCharsets.UTF_8));
    String token = "header." + b64Payload + ".signature";
    Files.write(tokenFile.toPath(), token.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void applyRequestMetadata_success() throws Exception {
    writeToken(3600); // Expires in 1 hour
    
    CallCredentials.MetadataApplier applier = mock(CallCredentials.MetadataApplier.class);
    CallCredentials.RequestInfo requestInfo = mock(CallCredentials.RequestInfo.class);
    Executor executor = Runnable::run; // direct executor

    creds.applyRequestMetadata(requestInfo, executor, applier);

    ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(applier).apply(metadataCaptor.capture());

    Metadata metadata = metadataCaptor.getValue();
    String token = metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER));
    assertThat(token).startsWith("Bearer header.");
  }
}
